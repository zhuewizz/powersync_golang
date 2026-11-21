// Package fixture 提供离线的假 PowerSync 服务，用于**双实现差分测试**：
// 把 testdata/sync/*.ndjson 按 /sync/stream 协议回放给客户端，
// 这样官方 JS SDK 与 Go 实现可以在没有真实服务/网络的环境（CI）里同步同一份数据。
package fixture

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// Server 是一份可回放的 fixture。
type Server struct {
	// Checkpoint 是 checkpoint 行原文。
	Checkpoint string
	// Complete 是 checkpoint_complete 行原文。
	Complete string
	// Data 是 data 行原文（按文件顺序）。
	Data []string
	// LastOpID 是 checkpoint 里的全局 last_op_id（仅日志/参考用）。
	LastOpID string
	// maxOp 是每个 bucket 已下发的最大 op_id，"已是最新"判定用它。
	maxOp map[string]int64
}

// Load 读取并解析 NDJSON fixture。
func Load(path string) (*Server, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return FromBytes(raw)
}

// FromBytes 解析内存里的 NDJSON fixture（示例程序用 go:embed 时用它）。
func FromBytes(raw []byte) (*Server, error) {
	s := &Server{maxOp: map[string]int64{}}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		switch {
		case strings.Contains(line, `"checkpoint_complete"`):
			s.Complete = line
		case strings.Contains(line, `"data"`):
			s.Data = append(s.Data, line)
			if err := s.trackMaxOp(line); err != nil {
				return nil, err
			}
		case strings.Contains(line, `"checkpoint"`):
			s.Checkpoint = line
			var cp struct {
				Checkpoint struct {
					LastOpID string `json:"last_op_id"`
				} `json:"checkpoint"`
			}
			if err := json.Unmarshal([]byte(line), &cp); err != nil {
				return nil, fmt.Errorf("fixture: 解析 checkpoint: %w", err)
			}
			s.LastOpID = cp.Checkpoint.LastOpID
		}
	}
	if s.Checkpoint == "" || s.Complete == "" || len(s.Data) == 0 {
		return nil, fmt.Errorf("fixture: 不完整（需要 checkpoint + data + checkpoint_complete）")
	}
	return s, nil
}

func (s *Server) trackMaxOp(line string) error {
	var dl struct {
		Data struct {
			Bucket string `json:"bucket"`
			Data   []struct {
				OpID string `json:"op_id"`
			} `json:"data"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(line), &dl); err != nil {
		return fmt.Errorf("fixture: 解析 data: %w", err)
	}
	for _, op := range dl.Data.Data {
		if v, ok := parseOpID(op.OpID); ok && v > s.maxOp[dl.Data.Bucket] {
			s.maxOp[dl.Data.Bucket] = v
		}
	}
	return nil
}

// Handler 返回实现 /sync/stream、/write-checkpoint2.json、/healthz 的 mux。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/write-checkpoint2.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":{"write_checkpoint":"1"}}`)
	})
	mux.HandleFunc("/sync/stream", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		var req struct {
			Buckets []struct {
				Name  string `json:"name"`
				After string `json:"after"`
			} `json:"buckets"`
		}
		_ = json.Unmarshal(body, &req)

		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, s.Checkpoint)
		if !s.upToDate(req.Buckets) {
			for _, d := range s.Data {
				_, _ = fmt.Fprintln(w, d)
			}
		}
		_, _ = fmt.Fprintln(w, s.Complete)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// 真实服务端会保持连接继续推送；这里也保持到客户端断开。
		<-r.Context().Done()
	})
	return mux
}

func (s *Server) upToDate(buckets []struct {
	Name  string `json:"name"`
	After string `json:"after"`
}) bool {
	if len(buckets) == 0 {
		return false
	}
	for _, b := range buckets {
		after, ok := parseOpID(b.After)
		if !ok || after < s.maxOp[b.Name] {
			return false
		}
	}
	return true
}

func parseOpID(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	return v, err == nil
}
