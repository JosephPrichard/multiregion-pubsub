package app

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
	"uuid"
)

var InternalServerError = errors.New("internal server error")
var BadRequestError = errors.New("payload is invalid")

func readJSON[T any](r *http.Request) (T, error) {
	defer r.Body.Close()

	var data T

	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		slog.ErrorContext(r.Context(), "failed to drain http request body", "error", err)
		return data, BadRequestError
	}
	str := string(bytes)

	// we can log all JSON bodies for simplicity, we can assume payloads being received are small for this use case (we already hold the whole thing in RAM)
	slog.InfoContext(r.Context(), "received request body", "str", str)

	if err := json.Unmarshal(bytes, &data); err != nil {
		return data, fmt.Errorf("failed to unmarshal json body: %s", err)
	}
	return data, nil
}

func writeJSON[V any](ctx context.Context, w http.ResponseWriter, data V, status int) {
	bytes, err := json.Marshal(data)
	if err != nil {
		slog.Error("failed to marshal json response", "error", err)
		http.Error(w, "internal server error: failed to marshal response", http.StatusInternalServerError)
		return
	}

	slog.InfoContext(ctx, "writing JSON response body", "response", string(bytes), "status", status)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(bytes); err != nil {
		slog.Error("failed to write json response", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

type Response struct {
	Payload any   `json:"payload"`
	Error   error `json:"error"`
	Status  int   `json:"status"`
}

type ServiceResponse struct {
	Message string `json:"message"`
}

func rest(h func(w http.ResponseWriter, r *http.Request) Response) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		start := time.Now()

		trace := r.Header.Get("X-trace")
		if trace == "" {
			trace = uuid.New().String()
		}
		r = r.WithContext(TracedContext(r.Context(), trace))

		slog.InfoContext(ctx, "received REST call", "method", r.Method, "path", r.URL.Path, "url", r.URL.String(), "headers", r.Header)

		response := h(w, r)

		err, isError := response.Payload.(error)
		if isError {
			response.Payload = ServiceResponse{Message: "Error: " + err.Error()}
		}

		writeJSON(ctx, w, response.Payload, response.Status)

		slog.InfoContext(ctx, "handled REST call", "error", response.Error, "path", r.URL.Path, "timeTaken", time.Since(start).String())
	}
}
