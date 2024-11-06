package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
)

var (
	ErrInternalServerError = errors.New("internal server error")
)

var transact TransactionLogger
var store *Store

func InitializeTransactionLog() (err error) {
	slog.Info("initializing transaction log")

	transact, err = NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create transaction logger: %w", err)
	}

	events, errors := transact.ReadEvents()
	event, channelOpen := Event{}, true

	for channelOpen && err == nil {
		select {
		case err, channelOpen = <-errors:
		case event, channelOpen = <-events:
			switch event.Type {
			case EventTypePut:
				err = store.Put(event.Key, event.Value)
			case EventTypeDelete:
				err = store.Delete(event.Key)
			}
		}
	}

	transact.Run()

	return err
}

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

	transact.WritePut(key, string(value))

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

	transact.WriteDelete(key)

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

	store = NewStore()
	if err := InitializeTransactionLog(); err != nil {
		log.Fatal(err)
	}

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
