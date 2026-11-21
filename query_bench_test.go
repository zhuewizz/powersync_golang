package powersync

import (
	"fmt"
	"testing"
)

// BenchmarkWatchChangeDetection 对比"整个结果集 fmt.Sprint" 与 64 位哈希的开销。
func BenchmarkWatchChangeDetection(b *testing.B) {
	rows := make([]map[string]any, 1000)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i), "title": "row", "done": int64(i % 2)}
	}

	b.Run("fmt.Sprint", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if fmt.Sprint(rows) == "" {
				b.Fatal("unreachable")
			}
		}
	})
	b.Run("hashRows", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if hashRows(rows) == 0 {
				b.Fatal("unreachable")
			}
		}
	})
}
