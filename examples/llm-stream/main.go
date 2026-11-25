// Command llm-stream streams fake LLM tokens to a browser with go-ssekit.
//
//	go run ./examples/llm-stream
//	open http://localhost:8080
//
// Swap fakeTokens for your model's streaming API to use it for real.
package main

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/shani8615/go-ssekit"
)

const answer = "Server-Sent Events are a simple way to push data from a Go server to the browser " +
	"over plain HTTP. Each token is flushed the moment it is ready, so users see the answer " +
	"appear as the model writes it, and EventSource reconnects automatically if the connection drops."

// fakeTokens emits words with a small delay, like a model would.
func fakeTokens(ctx context.Context) <-chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		for i, w := range strings.Fields(answer) {
			if i > 0 {
				w = " " + w
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(55 * time.Millisecond):
				out <- w
			}
		}
	}()
	return out
}

func main() {
	http.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(page))
	})
	http.HandleFunc("GET /stream", func(w http.ResponseWriter, r *http.Request) {
		s, err := ssekit.Upgrade(w, r, ssekit.StreamOptions{WriteTimeout: 10 * time.Second})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		n := 0
		for tok := range fakeTokens(s.Context()) {
			n++
			if err := s.SendJSON("token", map[string]any{"text": tok, "n": n}); err != nil {
				return // client went away
			}
		}
		_ = s.SendJSON("done", map[string]int{"tokens": n})
	})
	log.Println("listening on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil)) //nolint:gosec // demo server
}

const page = `<!doctype html>
<html><head><meta charset="utf-8"><title>go-ssekit · LLM streaming demo</title>
<style>
  body{margin:0;background:#181a21;color:#dcdfe4;font:16px/1.6 system-ui,-apple-system,Segoe UI,sans-serif;display:flex;justify-content:center}
  main{width:min(720px,92vw);padding:40px 0}
  h1{font-size:18px;font-weight:600;color:#9aa1ad;margin:0 0 20px}
  form{display:flex;gap:8px}
  input{flex:1;background:#242730;border:1px solid #343846;color:#dcdfe4;border-radius:8px;padding:10px 12px;font:inherit}
  button{background:#5cd687;color:#10331d;border:0;border-radius:8px;padding:0 18px;font:inherit;font-weight:600;cursor:pointer}
  #out{margin-top:22px;min-height:130px;background:#20232b;border:1px solid #2d313c;border-radius:10px;padding:16px 18px;white-space:pre-wrap}
  .cursor{display:inline-block;width:8px;height:1.1em;background:#5cd687;vertical-align:-3px;margin-left:2px;animation:b 1s steps(1) infinite}
  @keyframes b{50%{opacity:0}}
  #meta{margin-top:10px;color:#828997;font:13px ui-monospace,Menlo,monospace}
</style></head>
<body><main>
  <h1>go-ssekit · streaming tokens with EventSource</h1>
  <form id="f"><input id="q" value="What are Server-Sent Events?"><button>Ask</button></form>
  <div id="out"></div>
  <div id="meta">event: — · tokens: 0</div>
</main>
<script>
  const out = document.getElementById("out"), meta = document.getElementById("meta");
  document.getElementById("f").onsubmit = e => {
    e.preventDefault();
    out.textContent = ""; const cur = document.createElement("span"); cur.className = "cursor"; out.append(cur);
    const es = new EventSource("/stream?q=" + encodeURIComponent(document.getElementById("q").value));
    es.addEventListener("token", ev => {
      const d = JSON.parse(ev.data);
      cur.before(d.text);
      meta.textContent = "event: token · tokens: " + d.n;
    });
    es.addEventListener("done", ev => {
      meta.textContent = "event: done · tokens: " + JSON.parse(ev.data).tokens;
      cur.remove(); es.close();
    });
  };
</script>
</body></html>`
