// Package psclient 是仓库里几个命令行工具（psdiff/psdemo/psupload）共用的最小 HTTP 客户端：
// 从 PostgREST 风格的 RPC 取 PowerSync JWT，以及一个薄薄的 PostgREST CRUD 封装。
//
// 它**不属于库本体**（库使用者自己实现 Connector），只服务本仓库的工具。
package psclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultTimeout 是单次 HTTP 请求的默认超时。
const DefaultTimeout = 30 * time.Second

// errEmptyToken 表示 RPC 返回了空 token。
var errEmptyToken = errors.New("psclient: RPC 未返回 token")

// FetchToken 调用 PostgREST 风格的 `fetch_credentials` RPC 取 JWT。
//
//	rpcURL = http://host:8089/rest/rpc/fetch_credentials
//	body   = {"box_code": "...", "org_code": "..."}
func FetchToken(ctx context.Context, rpcURL, boxCode, orgCode string) (string, error) {
	if rpcURL == "" {
		return "", errors.New("psclient: 需要 RPC URL")
	}
	body := fmt.Sprintf(`{"box_code":%q,"org_code":%q}`, boxCode, orgCode)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rpcURL, strings.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("psclient: 调用 %s: %w", rpcURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<10))
		return "", fmt.Errorf("psclient: fetch_credentials HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("psclient: 解析 fetch_credentials: %w", err)
	}
	if out.Token == "" {
		return "", errEmptyToken
	}
	return out.Token, nil
}

// REST 是 PostgREST 的薄封装（只覆盖工具需要的几个操作）。
type REST struct {
	// Base 是 PostgREST base，如 http://host:8089/rest（不带尾部 /）。
	Base string
	// Token 是 PostgREST 用的 Bearer token（与 PowerSync JWT 相同）。
	Token  string
	Client *http.Client
}

func (r *REST) client() *http.Client {
	if r.Client != nil {
		return r.Client
	}
	return &http.Client{Timeout: DefaultTimeout}
}

func (r *REST) base() string { return strings.TrimRight(r.Base, "/") }

// HTTPError 是 PostgREST 返回的非 2xx 错误，带状态码，便于调用方区分"可重试/不可重试"。
type HTTPError struct {
	Status       int
	Method, Path string
	Body         string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("psclient: %s %s -> HTTP %d: %s", e.Method, e.Path, e.Status, e.Body)
}

// Fatal 报告该错误是否属于"重试也没用"的客户端错误（4xx）。
func (e *HTTPError) Fatal() bool { return e.Status >= 400 && e.Status < 500 }

// Do 发一个 JSON 请求；prefer 非空时作为 Prefer 头（如 "return=minimal"）。
func (r *REST) Do(ctx context.Context, method, path string, body any, prefer string) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, r.base()+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+r.Token)
	req.Header.Set("Content-Type", "application/json")
	if prefer != "" {
		req.Header.Set("Prefer", prefer)
	}
	resp, err := r.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<10))
		return &HTTPError{Status: resp.StatusCode, Method: method, Path: path,
			Body: strings.TrimSpace(string(msg))}
	}
	return nil
}

// GetJSON 读一个返回 JSON 数组/对象的端点（如 REST 表查询）。
func (r *REST) GetJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.base()+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+r.Token)
	req.Header.Set("Accept", "application/json")
	resp, err := r.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<10))
		return &HTTPError{Status: resp.StatusCode, Method: http.MethodGet, Path: path,
			Body: strings.TrimSpace(string(msg))}
	}
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	return dec.Decode(out)
}

// CountByID 统计某张表里 id 匹配的行数（工具里的清理/校验用）。
func (r *REST) CountByID(ctx context.Context, table, id string) (int, error) {
	var rows []map[string]any
	if err := r.GetJSON(ctx, tablePath(table)+"?id=eq."+url.QueryEscape(id)+"&select=id", &rows); err != nil {
		return 0, err
	}
	return len(rows), nil
}

// tablePath 把表名转成 URL 路径段。
func tablePath(table string) string { return "/" + url.PathEscape(table) }
