package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
)

type EventType int

const (
	EventTypePut EventType = iota + 1
	EventTypeDelete
)

type Event struct {
	Sequence uint64
	Type     EventType
	Key      string
	Value    string
}

type TransactionLogger interface {
	WritePut(key, value string)
	WriteDelete(key string)

	Err() <-chan error
	ReadEvents() (<-chan Event, <-chan error)
	Run()
}

type FileTransactionLogger struct {
	events       chan<- Event
	errors       <-chan error
	lastSequence uint64
	file         *os.File
}

func NewFileTransactionLogger(filename string) (logger TransactionLogger, err error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to open transaction log file: %w", err)
	}

	return &FileTransactionLogger{file: file}, nil
}

func (l *FileTransactionLogger) WritePut(key, value string) {
	l.events <- Event{Type: EventTypePut, Key: key, Value: value}
}

func (l *FileTransactionLogger) WriteDelete(key string) {
	l.events <- Event{Type: EventTypeDelete, Key: key}
}

func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *FileTransactionLogger) Run() {
	events := make(chan Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	go func() {
		for e := range events {
			l.lastSequence++

			if _, err := fmt.Fprintf(l.file, "%d\t%d\t%s\t\"%s\"\n", l.lastSequence, e.Type, e.Key, e.Value); err != nil {
				errors <- err
				return
			}
		}
	}()
}

func (l *FileTransactionLogger) ReadEvents() (eventsCh <-chan Event, errorsCh <-chan error) {
	scanner := bufio.NewScanner(l.file)
	outEvents := make(chan Event)
	outErrors := make(chan error, 1)

	go func() {
		var e Event

		defer close(outEvents)
		defer close(outErrors)

		for scanner.Scan() {
			line := scanner.Text()

			if _, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s",
				&e.Sequence, &e.Type, &e.Key, &e.Value); err != nil {
				outErrors <- fmt.Errorf("transaction log line parse error: %w", err)
				return
			}

			// Remove quotes from parsing
			e.Value = e.Value[1 : len(e.Value)-1]

			if l.lastSequence >= e.Sequence {
				outErrors <- fmt.Errorf("transaction number ouf of sequence")
				return
			}

			l.lastSequence = e.Sequence
			outEvents <- e
		}

		if err := scanner.Err(); !errors.Is(err, io.EOF) && err != nil {
			outErrors <- fmt.Errorf("transaction log read failure: %w", err)
			return
		}
	}()

	return outEvents, outErrors
}
