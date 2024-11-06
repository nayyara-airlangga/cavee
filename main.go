package main

import (
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"sync"
)

var (
	ErrNoSuchKey           = errors.New("no such key")
	ErrInternalServerError = errors.New("internal server error")
)

type Store struct {
	sync.RWMutex
	m map[string]string
}

func NewStore() *Store {
	return &Store{
		m: make(map[string]string),
	}
}

func (s *Store) Put(key, value string) (err error) {
	slog.Info("putting key to store", slog.String("key", key))

	s.Lock()
	s.m[key] = value
	s.Unlock()

	return nil
}

func (s *Store) Get(key string) (value string, err error) {
	slog.Info("getting value using key", slog.String("key", key))

	s.RLock()
	value, exists := s.m[key]
	s.RUnlock()

	if !exists {
		return "", ErrNoSuchKey
	}

	return value, nil
}

func (s *Store) Delete(key string) (err error) {
	slog.Info("deleting key from store", slog.String("key", key))

	s.Lock()
	delete(s.m, key)
	s.Unlock()

	return nil
}

var store = NewStore()

func PutHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		slog.Error(ErrInternalServerError.Error(), slog.String("error", err.Error()))
		http.Error(w, ErrInternalServerError.Error(), http.StatusInternalServerError)
		return
	}

	if err = store.Put(key, string(value)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func GetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	value, err := store.Get(key)
	if errors.Is(err, ErrNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, ErrInternalServerError.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))
}

func DeleteHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	err := store.Delete(key)
	if err != nil {
		http.Error(w, ErrInternalServerError.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func healthcheck(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK!"))
}

func main() {
	logOpts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	logHandler := slog.NewJSONHandler(os.Stdout, logOpts)
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	slog.Info("Starting up Cavee")

	router := http.NewServeMux()
	router.HandleFunc("/", healthcheck)

	router.HandleFunc("PUT /v1/key/{key}", PutHandler)
	router.HandleFunc("GET /v1/key/{key}", GetHandler)
	router.HandleFunc("DELETE /v1/key/{key}", DeleteHandler)

	server := &http.Server{
		Addr:    "0.0.0.0:8080",
		Handler: router,
	}

	log.Fatal(server.ListenAndServe())
}
