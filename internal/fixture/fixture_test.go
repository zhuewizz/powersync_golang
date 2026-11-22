package fixture_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/zhuewizz/powersync_golang/internal/fixture"
)

func loadServer(t *testing.T) *fixture.Server {
	t.Helper()
	s, err := fixture.Load("../../testdata/sync/apply-todos.ndjson")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return s
}

type streamRequest struct {
	Buckets []struct {
		Name  string `json:"name"`
		After string `json:"after"`
	} `json:"buckets"`
}

// stream 发一次 /sync/stream 请求并返回收到的全部行（请求方读够 checkpoint 后就断开）。
func stream(t *testing.T, srv *fixture.Server, req streamRequest) []map[string]json.RawMessage {
	t.Helper()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(ts.URL+"/sync/stream", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	var out []map[string]json.RawMessage
	dec := json.NewDecoder(resp.Body)
	for {
		var line map[string]json.RawMessage
		if err := dec.Decode(&line); err != nil {
			break // 服务端保持连接，读到 checkpoint_complete 后由 defer 关闭
		}
		out = append(out, line)
		if _, ok := line["checkpoint_complete"]; ok {
			break
		}
	}
	return out
}

func kinds(lines []map[string]json.RawMessage) []string {
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		for k := range l {
			out = append(out, k)
		}
	}
	return out
}

// TestFullSyncThenUpToDate 覆盖"从 0 拉全量"与"已是最新只回 checkpoint"两种行为。
func TestFullSyncThenUpToDate(t *testing.T) {
	srv := loadServer(t)

	full := stream(t, srv, streamRequest{})
	if got := strings.Join(kinds(full), ","); got != "checkpoint,data,checkpoint_complete" {
		t.Fatalf("全量行类型 = %s", got)
	}

	up := stream(t, srv, streamRequest{Buckets: []struct {
		Name  string `json:"name"`
		After string `json:"after"`
	}{{Name: `50#test_data["ORG-A"]`, After: "50"}}})
	if got := strings.Join(kinds(up), ","); got != "checkpoint,checkpoint_complete" {
		t.Fatalf("已是最新行类型 = %s（after 够大时不应再下发 data）", got)
	}
}

// TestOtherEndpoints 覆盖 /healthz 与 /write-checkpoint2.json。
func TestOtherEndpoints(t *testing.T) {
	ts := httptest.NewServer(loadServer(t).Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz = %d", resp.StatusCode)
	}

	resp, err = http.Get(ts.URL + "/write-checkpoint2.json?client_id=x")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Data struct {
			WriteCheckpoint string `json:"write_checkpoint"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("解析 write-checkpoint: %v", err)
	}
	if out.Data.WriteCheckpoint != "1" {
		t.Errorf("write_checkpoint = %q", out.Data.WriteCheckpoint)
	}
}

// TestLoadRejectsIncomplete 覆盖坏 fixture。
func TestLoadRejectsIncomplete(t *testing.T) {
	path := t.TempDir() + "/bad.ndjson"
	if err := osWriteFile(path, `{"checkpoint":{"last_op_id":"1","buckets":[]}}`); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Load(path); err == nil {
		t.Fatal("缺少 data/complete 应报错")
	}
}

func osWriteFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}
