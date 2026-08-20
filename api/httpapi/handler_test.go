package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ChesleyC/pay-ledger/internal/cluster"
	"github.com/ChesleyC/pay-ledger/internal/payment"
	"github.com/ChesleyC/pay-ledger/internal/storage"
)

type fakeService struct {
	payment   payment.Payment
	created   bool
	createErr error
	status    cluster.Status
}

func (f *fakeService) CreatePayment(payment.CreateRequest) (payment.Payment, bool, error) {
	return f.payment, f.created, f.createErr
}
func (f *fakeService) GetPayment(id string) (payment.Payment, error) {
	if id != f.payment.ID {
		return payment.Payment{}, storage.ErrNotFound
	}
	return f.payment, nil
}
func (f *fakeService) GetByIdempotencyKey(key string) (payment.Payment, error) {
	if key != f.payment.IdempotencyKey {
		return payment.Payment{}, storage.ErrNotFound
	}
	return f.payment, nil
}
func (f *fakeService) Status() cluster.Status { return f.status }

func TestCreatePaymentStatusCodes(t *testing.T) {
	p := payment.Payment{ID: "one", IdempotencyKey: "key"}
	for _, test := range []struct {
		name    string
		created bool
		code    int
	}{
		{"new", true, http.StatusCreated}, {"duplicate", false, http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeService{payment: p, created: test.created}
			body := bytes.NewBufferString(`{"idempotency_key":"key","account_id":"acct","amount_cents":100}`)
			response := httptest.NewRecorder()
			New(service).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/payments", body))
			if response.Code != test.code {
				t.Fatalf("got %d, want %d", response.Code, test.code)
			}
			var got payment.Payment
			if err := json.NewDecoder(response.Body).Decode(&got); err != nil || got.ID != p.ID {
				t.Fatalf("response = (%+v, %v)", got, err)
			}
		})
	}
}

func TestFollowerReturnsLeader(t *testing.T) {
	service := &fakeService{
		createErr: cluster.ErrNotLeader,
		status:    cluster.Status{LeaderID: "node2", LeaderHTTP: "http://node2:8080"},
	}
	body := bytes.NewBufferString(`{"idempotency_key":"key","account_id":"acct","amount_cents":100}`)
	response := httptest.NewRecorder()
	New(service).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/payments", body))
	if response.Code != http.StatusTemporaryRedirect {
		t.Fatalf("got %d", response.Code)
	}
	if got := response.Header().Get("Location"); got != "http://node2:8080/v1/payments" {
		t.Fatalf("Location = %q", got)
	}
}

func TestNotFound(t *testing.T) {
	service := &fakeService{createErr: errors.New("unused")}
	response := httptest.NewRecorder()
	New(service).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/payments/missing", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("got %d", response.Code)
	}
}
