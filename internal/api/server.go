package api

import (
	"log"
	"net/http"

	"github.com/Afrawles/miniQq/internal/queue"
)

type Config struct {
	RAddr         string
	TrustedOrigin string
}
type App struct {
	Config Config
	Store  *queue.RedisStore
}

func (app *App) Server() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("Ok"))
	})

	mux.HandleFunc("POST /api/jobs", app.HandleEnqueueJob)
	mux.HandleFunc("GET /api/jobs", app.HandleGetJobs)
	mux.HandleFunc("GET /api/queues/{name}/stats", app.HandleGetQueueStats)

	log.Fatal(http.ListenAndServe(":8080", app.corsMiddleware(mux)))
}

func (app *App) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", app.Config.TrustedOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
