package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/ChesleyC/pay-ledger/internal/cluster"
	"github.com/ChesleyC/pay-ledger/internal/payment"
	"github.com/ChesleyC/pay-ledger/internal/storage"
)

type PaymentService interface {
	CreatePayment(payment.CreateRequest) (payment.Payment, bool, error)
	GetPayment(string) (payment.Payment, error)
	GetByIdempotencyKey(string) (payment.Payment, error)
	Status() cluster.Status
}

type Handler struct {
	service PaymentService
}

func New(service PaymentService) http.Handler {
	h := &Handler{service: service}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/payments", h.createPayment)
	mux.HandleFunc("GET /v1/payments/{id}", h.getPayment)
	mux.HandleFunc("GET /v1/idempotency/{key}", h.getByIdempotencyKey)
	mux.HandleFunc("GET /v1/status", h.status)
	mux.HandleFunc("GET /health", h.health)
	return mux
}

func (h *Handler) createPayment(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	var request payment.CreateRequest
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if err := request.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	p, created, err := h.service.CreatePayment(request)
	if errors.Is(err, cluster.ErrNotLeader) {
		status := h.service.Status()
		if status.LeaderHTTP != "" {
			location := strings.TrimRight(status.LeaderHTTP, "/") + "/v1/payments"
			w.Header().Set("Location", location)
			w.Header().Set("X-PayLedger-Leader", status.LeaderHTTP)
		}
		writeJSON(w, http.StatusTemporaryRedirect, map[string]any{
			"error": "not leader", "leader_id": status.LeaderID, "leader_http_address": status.LeaderHTTP,
		})
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	writeJSON(w, code, p)
}

func (h *Handler) getPayment(w http.ResponseWriter, r *http.Request) {
	p, err := h.service.GetPayment(r.PathValue("id"))
	h.writePayment(w, p, err)
}

func (h *Handler) getByIdempotencyKey(w http.ResponseWriter, r *http.Request) {
	key, err := url.PathUnescape(r.PathValue("key"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid idempotency key")
		return
	}
	p, err := h.service.GetByIdempotencyKey(key)
	h.writePayment(w, p, err)
}

func (h *Handler) writePayment(w http.ResponseWriter, p payment.Payment, err error) {
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "payment not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (h *Handler) status(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.service.Status())
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		// Headers have already been sent; logging belongs in the outer HTTP server.
		_ = fmt.Errorf("encode response: %w", err)
	}
}
