package main

import (
	"errors"
	"log/slog"
	"sync"
)

var (
	ErrNoSuchKey = errors.New("no such key")
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
