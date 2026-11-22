// Command psfixture-server 启动离线假 PowerSync 服务（实现见 internal/fixture），
// 供 scripts/run-js-diff.sh 让 Go 与官方 JS SDK 同步同一份数据。
//
//	go run ./cmd/psfixture-server -addr 127.0.0.1:18080 -fixture testdata/sync/apply-todos.ndjson
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/zhuewizz/powersync_golang/internal/fixture"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "监听地址")
	path := flag.String("fixture", "testdata/sync/apply-todos.ndjson", "要回放的 NDJSON 文件")
	flag.Parse()

	srv, err := fixture.Load(*path)
	if err != nil {
		log.Fatalf("%v", err)
	}
	log.Printf("fixture server listening on http://%s（fixture=%s, last_op=%s, data 行=%d）",
		*addr, *path, srv.LastOpID, len(srv.Data))
	log.Fatal(http.ListenAndServe(*addr, srv.Handler()))
}
