package psclient_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zhuweizz/powersync_golang/internal/psclient"
)

// TestFetchToken 覆盖成功、空 token、非 200、非法 JSON 四条路径。
func TestFetchToken(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		switch r.URL.Path {
		case "/ok":
			_, _ = io.WriteString(w, `{"token":"jwt-value","org_code":"ORG001"}`)
		case "/empty":
			_, _ = io.WriteString(w, `{"token":""}`)
		case "/bad":
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":"nope"}`)
		case "/garbage":
			_, _ = io.WriteString(w, `not json`)
		}
	}))
	defer srv.Close()

	ctx := context.Background()
	tok, err := psclient.FetchToken(ctx, srv.URL+"/ok", "D1-2", "ORG001")
	if err != nil || tok != "jwt-value" {
		t.Fatalf("FetchToken = %q, %v", tok, err)
	}
	var parsed map[string]string
	if err := json.Unmarshal([]byte(gotBody), &parsed); err != nil {
		t.Fatalf("RPC body 不是 JSON: %s", gotBody)
	}
	if parsed["box_code"] != "D1-2" || parsed["org_code"] != "ORG001" {
		t.Errorf("RPC body = %s", gotBody)
	}

	if _, err := psclient.FetchToken(ctx, srv.URL+"/empty", "b", "o"); err == nil {
		t.Error("空 token 应报错")
	}
	if _, err := psclient.FetchToken(ctx, srv.URL+"/bad", "b", "o"); err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("非 200 错误应包含状态码: %v", err)
	}
	if _, err := psclient.FetchToken(ctx, srv.URL+"/garbage", "b", "o"); err == nil {
		t.Error("非法 JSON 应报错")
	}
	if _, err := psclient.FetchToken(ctx, "", "b", "o"); err == nil {
		t.Error("空 RPC URL 应报错")
	}
}

// TestREST 覆盖 Do / GetJSON / CountByID 与错误路径。
func TestREST(t *testing.T) {
	var lastMethod, lastPath, lastAuth, lastPrefer string
	var lastBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastMethod, lastPath = r.Method, r.URL.Path+"?"+r.URL.RawQuery
		lastAuth, lastPrefer = r.Header.Get("Authorization"), r.Header.Get("Prefer")
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&lastBody)
		}
		switch {
		case strings.Contains(r.URL.Path, "/fail"):
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"message":"bad"}`)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/todos"):
			// 有 id 过滤 → 返回 1 行；否则 3 行
			if strings.Contains(r.URL.RawQuery, "id=eq.") {
				_, _ = io.WriteString(w, `[{"id":"a"}]`)
			} else {
				_, _ = io.WriteString(w, `[{"id":"a"},{"id":"b"},{"id":"c"}]`)
			}
		default:
			w.WriteHeader(http.StatusCreated)
		}
	}))
	defer srv.Close()

	rest := &psclient.REST{Base: srv.URL + "/", Token: "tok"}
	ctx := context.Background()

	if err := rest.Do(ctx, http.MethodPost, "/todos", map[string]any{"id": "a"}, "resolution=merge-duplicates,return=minimal"); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if lastMethod != http.MethodPost || !strings.HasPrefix(lastPath, "/todos?") {
		t.Errorf("method/path = %s %s", lastMethod, lastPath)
	}
	if lastAuth != "Bearer tok" || lastPrefer == "" || lastBody["id"] != "a" {
		t.Errorf("auth/prefer/body = %q %q %v", lastAuth, lastPrefer, lastBody)
	}

	var rows []map[string]any
	if err := rest.GetJSON(ctx, "/todos?select=*", &rows); err != nil || len(rows) != 3 {
		t.Fatalf("GetJSON = %d rows, %v", len(rows), err)
	}
	if n, err := rest.CountByID(ctx, "todos", "a"); err != nil || n != 1 {
		t.Fatalf("CountByID = %d, %v", n, err)
	}
	if err := rest.Do(ctx, http.MethodDelete, "/fail", nil, ""); err == nil || !strings.Contains(err.Error(), "400") {
		t.Errorf("错误路径应包含状态码: %v", err)
	}
}
