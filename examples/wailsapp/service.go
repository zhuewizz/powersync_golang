package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wailsapp/wails/v3/pkg/application"

	powersync "msmkSync"
	"msmkSync/examples/cutterschema"
	"msmkSync/restconn"
)

// aliasTables 是同步规则里用短别名（AS i 等）的表：服务端下发的 object_type 就是别名。
var aliasTables = map[string]bool{"d": true, "h": true, "i": true, "l": true, "r": true, "s": true, "t": true}

// Config 是服务参数（默认值写死在本机测试环境，可用环境变量覆盖）。
type Config struct {
	DBPath   string
	Endpoint string
	RPC      string
	Box      string
	Org      string
	REST     string
}

// Service 是暴露给前端的服务（Wails v3 会把导出的方法绑定成 JS 函数）。
type Service struct {
	cfg Config
	log *slog.Logger

	mu       sync.Mutex
	app      *application.App
	db       *powersync.DB
	conn     *restconn.Connector
	readOnly bool
	msg      string

	watchCancel context.CancelFunc
	watchTable  string

	stop   context.CancelFunc
	stopCh chan struct{}
}

// Status 是顶部状态栏的数据。
type Status struct {
	Ready        bool   `json:"ready"`
	ReadOnly     bool   `json:"readOnly"`
	Connected    bool   `json:"connected"`
	Cursor       string `json:"cursor"`
	Buckets      int    `json:"buckets"`
	UploadQueue  int64  `json:"uploadQueue"`
	Online       bool   `json:"online"`
	LatencyMS    int64  `json:"latencyMs"`
	LastSyncedAt string `json:"lastSyncedAt"`
	Skipped      int64  `json:"skipped"`
	DBPath       string `json:"dbPath"`
	Message      string `json:"message"`
}

// TableInfo 是左侧表清单的一项。
type TableInfo struct {
	Name  string `json:"name"`
	Rows  int64  `json:"rows"`
	Alias bool   `json:"alias"`
}

// ColumnInfo 描述一列，前端据此渲染表格与表单。
type ColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
	IsID bool   `json:"isID"`
}

// RowsPage 是一页数据。
type RowsPage struct {
	Table   string           `json:"table"`
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Total   int64            `json:"total"`
	Offset  int              `json:"offset"`
	Limit   int              `json:"limit"`
}

// CheckResult 是“本地 vs 后端”行数核对结果（稳定性测试用）。
type CheckResult struct {
	Table  string `json:"table"`
	Local  int64  `json:"local"`
	Remote int64  `json:"remote"`
	OK     bool   `json:"ok"`
	Note   string `json:"note"`
}

// NewService 构造服务（不会打开数据库，Start 才打开）。
func NewService(cfg Config) *Service {
	return &Service{
		cfg:    cfg,
		log:    slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
		stopCh: make(chan struct{}),
	}
}

// AttachApp 让服务能向前端推事件。
func (s *Service) AttachApp(app *application.App) {
	s.mu.Lock()
	s.app = app
	s.mu.Unlock()
}

// Start 打开本地库并连接同步（后台执行，不阻塞 UI）。
func (s *Service) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.stop = cancel

	go func() {
		s.mu.Lock()
		s.msg = "正在打开本地库…"
		s.mu.Unlock()
		s.emitStatus()

		conn := restconn.New(restconn.Config{
			Endpoint: s.cfg.Endpoint, RPC: s.cfg.RPC, Box: s.cfg.Box, Org: s.cfg.Org,
			RESTBase: s.cfg.REST, Logger: s.log,
		})
		opts := powersync.Options{
			Filename:    s.cfg.DBPath,
			Schema:      cutterschema.Schema(),
			Connector:   conn,
			LockTimeout: 3 * time.Second,
		}
		db, err := powersync.Open(ctx, opts)
		readOnly := false
		if err != nil && strings.Contains(err.Error(), "locked") {
			// 另一个实例正在同步：降级只读（WAL 多读者），避免两个引擎写同一个库。
			opts.NoFileLock = true
			opts.Connector = nil
			if db, err = powersync.Open(ctx, opts); err == nil {
				readOnly = true
			}
		}
		if err != nil {
			s.mu.Lock()
			s.msg = "打开数据库失败: " + err.Error()
			s.mu.Unlock()
			s.log.Error("打开数据库失败", "err", err)
			s.emitStatus()
			return
		}

		s.mu.Lock()
		s.db = db
		s.conn = conn
		s.readOnly = readOnly
		if readOnly {
			s.msg = "另一个实例正在同步，本窗口为只读浏览"
		} else {
			s.msg = "正在连接同步…"
		}
		s.mu.Unlock()
		s.emitStatus()

		if !readOnly {
			if err := db.Connect(ctx); err != nil {
				s.mu.Lock()
				s.msg = "连接同步失败: " + err.Error()
				s.mu.Unlock()
				s.log.Error("Connect 失败", "err", err)
				s.emitStatus()
			} else {
				s.mu.Lock()
				s.msg = "等待首次同步…"
				s.mu.Unlock()
				s.emitStatus()
				waitCtx, waitCancel := context.WithTimeout(ctx, 2*time.Minute)
				defer waitCancel()
				if err := db.WaitForFirstSync(waitCtx); err != nil {
					s.mu.Lock()
					s.msg = "首次同步超时: " + err.Error()
					s.mu.Unlock()
				} else {
					s.mu.Lock()
					s.msg = ""
					s.mu.Unlock()
				}
			}
		}

		// 每秒推一次状态
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.emitStatus()
			}
		}
	}()
}

// Stop 断开同步并关闭库。
func (s *Service) Stop() {
	if s.stop != nil {
		s.stop()
	}
	s.mu.Lock()
	db := s.db
	s.db = nil
	s.mu.Unlock()
	if db != nil {
		db.Disconnect()
		_ = db.Close(context.Background())
	}
}

// ---------------------------------------------------------------- 前端可调用的方法

// Status 返回当前同步状态（前端也可监听 "sync:status" 事件）。
func (s *Service) Status() Status {
	return s.status()
}

// Tables 返回所有表的行数（按行数倒序）。
func (s *Service) Tables() ([]TableInfo, error) {
	db, err := s.database()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	tables := cutterschema.Schema().Tables()
	out := make([]TableInfo, 0, len(tables))
	for _, t := range tables {
		n, err := s.countRows(ctx, db, t.Name)
		if err != nil {
			continue
		}
		out = append(out, TableInfo{Name: t.Name, Rows: n, Alias: aliasTables[t.Name]})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Rows != out[j].Rows {
			return out[i].Rows > out[j].Rows
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// Columns 返回一张表的列定义（含类型，前端用来做表单）。
func (s *Service) Columns(table string) ([]ColumnInfo, error) {
	t, err := findTable(table)
	if err != nil {
		return nil, err
	}
	out := make([]ColumnInfo, 0, len(t.Columns)+1)
	out = append(out, ColumnInfo{Name: "id", Type: "TEXT", IsID: true})
	for _, c := range t.Columns {
		out = append(out, ColumnInfo{Name: c.Name, Type: string(c.Type)})
	}
	return out, nil
}

// Rows 返回一页数据。
func (s *Service) Rows(table string, limit, offset int) (*RowsPage, error) {
	db, err := s.database()
	if err != nil {
		return nil, err
	}
	if _, err := findTable(table); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 2000 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	total, err := s.countRows(ctx, db, table)
	if err != nil {
		return nil, err
	}
	rows, err := db.GetAll(ctx, fmt.Sprintf(`SELECT * FROM %s ORDER BY id LIMIT %d OFFSET %d`,
		quoteIdent(table), limit, offset))
	if err != nil {
		return nil, err
	}

	cols := make([]string, 0, 8)
	seen := map[string]struct{}{}
	for _, r := range rows {
		for k := range r {
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			cols = append(cols, k)
		}
	}
	sort.SliceStable(cols, func(i, j int) bool {
		if cols[i] == "id" {
			return true
		}
		if cols[j] == "id" {
			return false
		}
		return cols[i] < cols[j]
	})
	return &RowsPage{Table: table, Columns: cols, Rows: rows, Total: total, Offset: offset, Limit: limit}, nil
}

// Insert 新增一行（id 未给则自动生成 UUIDv4），并触发上传。
func (s *Service) Insert(table string, values map[string]any) (string, error) {
	db, err := s.database()
	if err != nil {
		return "", err
	}
	if s.isReadOnly() {
		return "", fmt.Errorf("当前是只读模式（另一个实例在同步），无法写入")
	}
	t, err := findTable(table)
	if err != nil {
		return "", err
	}

	id, _ := values["id"].(string)
	if strings.TrimSpace(id) == "" {
		id = newID()
	}
	delete(values, "id")

	cols := []string{"id"}
	args := []any{id}
	names := make([]string, 0, len(values))
	for k := range values {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		col, ok := findColumn(t, k)
		if !ok {
			return "", fmt.Errorf("表 %s 没有列 %q", table, k)
		}
		cols = append(cols, quoteIdent(k))
		args = append(args, coerce(col.Type, values[k]))
	}

	sqlStr := fmt.Sprintf(`INSERT INTO %s(%s) VALUES(%s)`,
		quoteIdent(table), strings.Join(cols, ", "), placeholders(len(cols)))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.Execute(ctx, sqlStr, args...); err != nil {
		return "", err
	}
	s.emitDataChanged(table)
	return id, nil
}

// Update 按 id 更新若干列（值为 null 表示置空）。
func (s *Service) Update(table, id string, values map[string]any) (int64, error) {
	db, err := s.database()
	if err != nil {
		return 0, err
	}
	if s.isReadOnly() {
		return 0, fmt.Errorf("当前是只读模式（另一个实例在同步），无法写入")
	}
	t, err := findTable(table)
	if err != nil {
		return 0, err
	}
	if len(values) == 0 {
		return 0, fmt.Errorf("没有要更新的列")
	}

	names := make([]string, 0, len(values))
	for k := range values {
		names = append(names, k)
	}
	sort.Strings(names)

	sets := make([]string, 0, len(names))
	args := make([]any, 0, len(names)+1)
	for _, k := range names {
		col, ok := findColumn(t, k)
		if !ok {
			return 0, fmt.Errorf("表 %s 没有列 %q", table, k)
		}
		sets = append(sets, quoteIdent(k)+" = ?")
		args = append(args, coerce(col.Type, values[k]))
	}
	args = append(args, id)

	sqlStr := fmt.Sprintf(`UPDATE %s SET %s WHERE id = ?`, quoteIdent(table), strings.Join(sets, ", "))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	n, err := db.Execute(ctx, sqlStr, args...)
	if err != nil {
		return 0, err
	}
	s.emitDataChanged(table)
	return n, nil
}

// Delete 按 id 删除一行。
func (s *Service) Delete(table, id string) (int64, error) {
	db, err := s.database()
	if err != nil {
		return 0, err
	}
	if s.isReadOnly() {
		return 0, fmt.Errorf("当前是只读模式（另一个实例在同步），无法写入")
	}
	if _, err := findTable(table); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	n, err := db.Execute(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id = ?`, quoteIdent(table)), id)
	if err != nil {
		return 0, err
	}
	s.emitDataChanged(table)
	return n, nil
}

// Watch 监听一张表的本地变化（同步下来的 + 本地写的），有变化就推 "data:changed"。
func (s *Service) Watch(table string) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	if _, err := findTable(table); err != nil {
		return err
	}
	s.mu.Lock()
	if s.watchCancel != nil {
		s.watchCancel()
		s.watchCancel = nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.watchCancel = cancel
	s.watchTable = table
	s.mu.Unlock()

	ch, err := db.WatchTables(ctx, []string{table},
		fmt.Sprintf(`SELECT count(*) AS n FROM %s`, quoteIdent(table)))
	if err != nil {
		cancel()
		return err
	}
	go func() {
		for range ch {
			s.emitDataChanged(table)
		}
	}()
	return nil
}

// Unwatch 停止监听。
func (s *Service) Unwatch() {
	s.mu.Lock()
	if s.watchCancel != nil {
		s.watchCancel()
		s.watchCancel = nil
	}
	s.watchTable = ""
	s.mu.Unlock()
}

// Check 核对一张表的“本地行数 vs 后端行数”（稳定性测试：点一下就知道同步是否跟上）。
func (s *Service) Check(table string) (*CheckResult, error) {
	db, err := s.database()
	if err != nil {
		return nil, err
	}
	if _, err := findTable(table); err != nil {
		return nil, err
	}
	if s.readOnly {
		return &CheckResult{Table: table, Note: "只读模式，未连接后端"}, nil
	}
	if err := s.conn.EnsureToken(context.Background()); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	local, err := s.countRows(ctx, db, table)
	if err != nil {
		return nil, err
	}
	res := &CheckResult{Table: table, Local: local, Remote: -1}
	var raw []map[string]json.RawMessage
	if err := s.conn.REST().GetJSON(ctx, "/"+table+"?select=id&limit=200000", &raw); err != nil {
		res.Note = "后端无此表/REST 不可达: " + err.Error()
		return res, nil
	}
	res.Remote = int64(len(raw))
	res.OK = local <= res.Remote // 后端可能因 sync rules 多于本地
	switch {
	case local == res.Remote:
		res.Note = "本地与后端行数一致"
	case res.OK:
		res.Note = fmt.Sprintf("后端多 %d 行（sync rules 过滤/其它端写入，正常）", res.Remote-local)
	default:
		res.Note = fmt.Sprintf("本地多 %d 行：可能还没上传完/上传失败", local-res.Remote)
	}
	return res, nil
}

// SyncNow 重新连接同步（用于断线后手动重连）。
func (s *Service) SyncNow() error {
	s.mu.Lock()
	db := s.db
	readOnly := s.readOnly
	s.mu.Unlock()
	if db == nil {
		return fmt.Errorf("数据库还没打开")
	}
	if readOnly {
		return fmt.Errorf("只读模式（另一个实例在同步）")
	}
	db.Disconnect()
	if err := db.Connect(context.Background()); err != nil {
		return err
	}
	return nil
}

// ---------------------------------------------------------------- 内部

func (s *Service) database() (*powersync.DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		if s.msg != "" {
			return nil, fmt.Errorf("%s", s.msg)
		}
		return nil, fmt.Errorf("数据库还没打开")
	}
	return s.db, nil
}

func (s *Service) isReadOnly() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readOnly
}

func (s *Service) status() Status {
	s.mu.Lock()
	db, readOnly, msg := s.db, s.readOnly, s.msg
	s.mu.Unlock()

	st := Status{Ready: db != nil, ReadOnly: readOnly, DBPath: s.cfg.DBPath, Message: msg}
	if db == nil {
		return st
	}
	cur := db.CurrentStatus()
	net := db.CurrentNetworkStatus()
	st.Connected = cur.Connected
	st.Cursor = cur.Cursor
	st.Buckets = cur.Buckets
	if !cur.LastSyncedAt.IsZero() {
		st.LastSyncedAt = cur.LastSyncedAt.Format("15:04:05")
	}
	st.Online = net.Online
	st.LatencyMS = net.Latency.Milliseconds()
	st.Skipped = db.SkippedUnknownOps()
	if n, err := db.UploadQueue(context.Background()); err == nil {
		st.UploadQueue = n
	}
	return st
}

func (s *Service) emitStatus() {
	s.mu.Lock()
	app := s.app
	s.mu.Unlock()
	if app == nil {
		return
	}
	app.Event.Emit("sync:status", s.status())
}

func (s *Service) emitDataChanged(table string) {
	s.mu.Lock()
	app := s.app
	s.mu.Unlock()
	if app == nil {
		return
	}
	app.Event.Emit("data:changed", table)
}

func (s *Service) countRows(ctx context.Context, db *powersync.DB, table string) (int64, error) {
	if rows, err := db.GetAll(ctx, fmt.Sprintf(`SELECT count(*) AS n FROM %s WHERE deleted_at IS NULL`, quoteIdent(table))); err == nil && len(rows) == 1 {
		return toInt64(rows[0]["n"]), nil
	}
	rows, err := db.GetAll(ctx, fmt.Sprintf(`SELECT count(*) AS n FROM %s`, quoteIdent(table)))
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, fmt.Errorf("count 查询没有结果")
	}
	return toInt64(rows[0]["n"]), nil
}

func findTable(name string) (powersync.Table, error) {
	for _, t := range cutterschema.Schema().Tables() {
		if t.Name == name {
			return t, nil
		}
	}
	return powersync.Table{}, fmt.Errorf("schema 里没有表 %q", name)
}

func findColumn(t powersync.Table, name string) (powersync.Column, bool) {
	for _, c := range t.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return powersync.Column{}, false
}

// coerce 按列类型把前端传来的值转成合适的 Go 类型（JSON 数字默认 float64）。
func coerce(colType powersync.ColumnType, v any) any {
	switch colType {
	case powersync.IntegerColumn:
		switch x := v.(type) {
		case float64:
			return int64(x)
		case int64:
			return x
		case string:
			if n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64); err == nil {
				return n
			}
			return x
		}
	case powersync.RealColumn:
		switch x := v.(type) {
		case float64:
			return x
		case int64:
			return float64(x)
		case string:
			if f, err := strconv.ParseFloat(strings.TrimSpace(x), 64); err == nil {
				return f
			}
			return x
		}
	}
	if s, ok := v.(string); ok {
		return s
	}
	return v
}

func placeholders(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ", ")
}

func quoteIdent(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}

// newID 生成 UUIDv4（真实后端主键多为 uuid 列）。
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("00000000-0000-4000-8000-%012d", time.Now().UnixNano()%1e12)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func defaultDBPath() string {
	// 放在 exe 同级的 data 目录；开发时就是 examples/wailsapp/data
	dir, err := os.UserConfigDir()
	if err != nil {
		return "powersync-wails.db"
	}
	return filepath.Join(dir, "powersync-wails", "app.db")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
