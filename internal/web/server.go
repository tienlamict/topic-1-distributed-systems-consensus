// Package web phục vụ UI và API chạy kịch bản.
//
// Không dùng WebSocket: server chạy TRỌN VẸN mô phỏng (mất vài ms vì thời gian
// là ảo) rồi trả cả trace về một lần. Browser tự replay. Nhờ vậy tua ngược,
// tua nhanh và nhảy tới thời điểm bất kỳ đều là thao tác cục bộ, tức thì.
package web

import (
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/tienlamict/topic1-consensus-demo/internal/scenario"
)

func noCache(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, must-revalidate")
		h.ServeHTTP(w, r)
	})
}

//go:embed assets
var assets embed.FS

// assetFS trả về nguồn asset. Khi chạy `go run` từ thư mục gốc repo, thư mục
// asset có thật trên đĩa nên ta phục vụ trực tiếp từ đó — sửa HTML/CSS/JS chỉ
// cần F5, không phải build lại. Binary đã đóng gói thì dùng bản embed.
func assetFS() (http.FileSystem, bool) {
	const dir = "internal/web/assets"
	if st, err := os.Stat(dir); err == nil && st.IsDir() {
		return http.Dir(dir), true
	}
	sub, err := fs.Sub(assets, "assets")
	if err != nil {
		panic(err)
	}
	return http.FS(sub), false
}

func Handler() http.Handler {
	mux := http.NewServeMux()

	src, dev := assetFS()
	if dev {
		log.Println("chế độ dev: phục vụ asset từ internal/web/assets (sửa xong chỉ cần F5)")
	}
	mux.Handle("/", noCache(http.FileServer(src)))

	mux.HandleFunc("/api/scenarios", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, scenario.All)
	})

	mux.HandleFunc("/api/run", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		id := q.Get("scenario")
		seed, _ := strconv.ParseInt(q.Get("seed"), 10, 64)

		params := map[string]int{}
		for k, v := range q {
			if len(k) > 2 && k[:2] == "p_" {
				if n, err := strconv.Atoi(v[0]); err == nil {
					params[k[2:]] = n
				}
			}
		}

		res, err := scenario.Run(id, seed, params)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, res)
	})

	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
