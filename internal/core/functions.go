package core

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	sqlite3 "modernc.org/sqlite"
)

// Version 是本地 core 兼容层的版本号，供 powersync_version() / powersync_rs_version() 返回。
// 官方 SDK 只用它做日志/兼容性提示，不参与协议。
const Version = "powersync-go/0.1.0"

// 与官方 core 同名的 SQL 函数在包初始化时全局注册（modernc 的函数注册是进程级的，
// 对之后新建的连接生效）。powersync_client_id() 需要每个库各自的值，而标量函数拿不到
// 数据库句柄，因此这里用"最后打开/使用"的 client id（单库进程下等价）。
var activeClientID atomic.Value

func init() {
	activeClientID.Store("")
	must := func(name string, n int32, fn func(*sqlite3.FunctionContext, []driver.Value) (driver.Value, error)) {
		if err := sqlite3.RegisterDeterministicScalarFunction(name, n, fn); err != nil {
			panic(fmt.Sprintf("core: 注册 SQL 函数 %s 失败: %v", name, err))
		}
	}
	must("powersync_client_id", 0, func(_ *sqlite3.FunctionContext, _ []driver.Value) (driver.Value, error) {
		if v, ok := activeClientID.Load().(string); ok {
			return v, nil
		}
		return "", nil
	})
	must("powersync_version", 0, func(_ *sqlite3.FunctionContext, _ []driver.Value) (driver.Value, error) {
		return Version, nil
	})
	must("powersync_rs_version", 0, func(_ *sqlite3.FunctionContext, _ []driver.Value) (driver.Value, error) {
		return Version, nil
	})
	must("powersync_diff", 2, func(_ *sqlite3.FunctionContext, args []driver.Value) (driver.Value, error) {
		oldJSON, err := asText(args, 0)
		if err != nil {
			return nil, err
		}
		newJSON, err := asText(args, 1)
		if err != nil {
			return nil, err
		}
		return DiffObjects(oldJSON, newJSON)
	})
	must("powersync_json_merge", -1, func(_ *sqlite3.FunctionContext, args []driver.Value) (driver.Value, error) {
		parts := make([]string, 0, len(args))
		for _, a := range args {
			if a == nil {
				continue
			}
			s, ok := a.(string)
			if !ok {
				return nil, fmt.Errorf("powersync_json_merge: 参数必须是 JSON 文本")
			}
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
				return nil, fmt.Errorf("powersync_json_merge: 期望 JSON 对象，收到 %q", s)
			}
			parts = append(parts, s[1:len(s)-1])
		}
		return "{" + strings.Join(parts, ",") + "}", nil
	})
}

// setActiveClientID 让 powersync_client_id() 返回该库的 client id。
func setActiveClientID(id string) { activeClientID.Store(id) }

func asText(args []driver.Value, i int) (string, error) {
	if i >= len(args) || args[i] == nil {
		return "", fmt.Errorf("core: 参数 %d 为空", i)
	}
	switch v := args[i].(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

// DiffObjects 实现 core 的 powersync_diff(old, new)：
//
//   - 先去掉两侧的 null 字段；
//   - old 为空（INSERT 场景）→ 直接返回 new（即"非空列"）；
//   - old 有、new 没有的键 → new 里补 null（表示该字段被清空）；
//   - 只保留取值不同的键（值为 new 侧的值）。
func DiffObjects(oldJSON, newJSON string) (string, error) {
	var oldObj, newObj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(oldJSON), &oldObj); err != nil {
		return "", fmt.Errorf("powersync_diff: 旧值不是 JSON 对象: %w", err)
	}
	if err := json.Unmarshal([]byte(newJSON), &newObj); err != nil {
		return "", fmt.Errorf("powersync_diff: 新值不是 JSON 对象: %w", err)
	}
	oldObj = dropNulls(oldObj)
	newObj = dropNulls(newObj)

	if len(oldObj) == 0 {
		return marshalSorted(newObj)
	}
	for key := range oldObj {
		if _, ok := newObj[key]; !ok {
			newObj[key] = json.RawMessage("null")
		}
	}
	changed := make(map[string]json.RawMessage)
	for key, nv := range newObj {
		ov, ok := oldObj[key]
		if !ok || !rawEqual(ov, nv) {
			changed[key] = nv
		}
	}
	return marshalSorted(changed)
}

func dropNulls(m map[string]json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(m))
	for k, v := range m {
		if string(bytesTrim(v)) == "null" {
			continue
		}
		out[k] = v
	}
	return out
}

func bytesTrim(b []byte) []byte {
	i, j := 0, len(b)
	for i < j && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
		i++
	}
	for j > i && (b[j-1] == ' ' || b[j-1] == '\t' || b[j-1] == '\n' || b[j-1] == '\r') {
		j--
	}
	return b[i:j]
}

func rawEqual(a, b json.RawMessage) bool {
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return string(a) == string(b)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return string(a) == string(b)
	}
	ab, _ := json.Marshal(av)
	bb, _ := json.Marshal(bv)
	return string(ab) == string(bb)
}

// marshalSorted 输出键有序的紧凑 JSON（便于测试断言与稳定落盘）。
func marshalSorted(m map[string]json.RawMessage) (string, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, err := json.Marshal(k)
		if err != nil {
			return "", err
		}
		b.Write(kb)
		b.WriteByte(':')
		b.Write(m[k])
	}
	b.WriteByte('}')
	return b.String(), nil
}

// quoteJSONPath 生成 json_extract 的路径字面量（'$.name'）。
func quoteJSONPath(name string) string {
	return sqlString("$." + name)
}
