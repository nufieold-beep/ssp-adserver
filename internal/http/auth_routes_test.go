package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestAuthStatusOpenWhenAPIKeyNotConfigured(t *testing.T) {
	t.Setenv("SSP_API_KEY", "")
	app := fiber.New()
	registerAuthRoutes(app, newStore())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/status", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected fiber test error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}
	defer resp.Body.Close()

	var body struct {
		Required      bool `json:"required"`
		Authenticated bool `json:"authenticated"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body.Required {
		t.Fatal("expected auth status to report auth not required")
	}
	if !body.Authenticated {
		t.Fatal("expected auth status to report authenticated when auth is not required")
	}
}

func TestAuthStatusRequiresSessionWhenAPIKeyConfigured(t *testing.T) {
	t.Setenv("SSP_API_KEY", "secret-key")
	app := fiber.New()
	registerAuthRoutes(app, newStore())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/status", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected fiber test error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}
	defer resp.Body.Close()

	var body struct {
		Required      bool `json:"required"`
		Authenticated bool `json:"authenticated"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !body.Required {
		t.Fatal("expected auth status to report auth required")
	}
	if body.Authenticated {
		t.Fatal("expected auth status to report unauthenticated without a session or API key")
	}
}

func TestAuthStatusAcceptsDashboardSessionCookie(t *testing.T) {
	t.Setenv("SSP_API_KEY", "secret-key")
	s := newStore()
	token, err := s.createDashboardSession()
	if err != nil {
		t.Fatalf("failed to create dashboard session: %v", err)
	}

	app := fiber.New()
	registerAuthRoutes(app, s)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/status", nil)
	req.AddCookie(&http.Cookie{Name: dashboardSessionCookieName, Value: token})
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected fiber test error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}
	defer resp.Body.Close()

	var body struct {
		Required      bool `json:"required"`
		Authenticated bool `json:"authenticated"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !body.Required {
		t.Fatal("expected auth status to report auth required")
	}
	if !body.Authenticated {
		t.Fatal("expected auth status to report authenticated with a valid session cookie")
	}
}
