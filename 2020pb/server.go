package downloader

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-chi/chi"
)

// Server receives webhooks from github to trigger a repull
// of CSV data
type Server struct {
}

// ServerOpts is used to configure the server connection
type ServerOpts struct {
	ListenAddress string
}

// NewServer returns a new server
func NewServer(opts *ServerOpts) *Server {
	router := chi.NewRouter()
	router.HandleFunc("/github/webhook/payload", func(w http.ResponseWriter, r *http.Request) {
		var out map[interface{}]interface{}
		if err := json.NewDecoder(r.Body).Decode(&out); err != nil {
			handleErr(err, w)
			return
		}
		log.Println("new rpayload received: ", out)
	})
	return nil
}

func handleErr(err error, w http.ResponseWriter) {
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(err.Error()))
}
