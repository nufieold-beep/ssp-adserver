package http

import (
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// AdminAPIKey returns middleware that requires a valid API key for admin routes.
// The key is read from the SSP_API_KEY environment variable.
// If SSP_API_KEY is not set, all admin requests are rejected.
func AdminAPIKey() fiber.Handler {
	key := os.Getenv("SSP_API_KEY")
	return func(c *fiber.Ctx) error {
		if key == "" {
			return c.Status(503).JSON(fiber.Map{"error": "admin API key not configured"})
		}
		auth := c.Get("Authorization")
		if auth == "" {
			auth = c.Query("api_key")
		}
		if auth != "Bearer "+key && auth != key {
			return c.Status(401).JSON(fiber.Map{"error": "unauthorized"})
		}
		return c.Next()
	}
}

// SecurityHeaders adds professional SSP security headers to every response.
func SecurityHeaders() fiber.Handler {
	return func(c *fiber.Ctx) error {
		c.Set("X-Content-Type-Options", "nosniff")
		c.Set("X-Frame-Options", "SAMEORIGIN")
		c.Set("X-XSS-Protection", "1; mode=block")
		c.Set("Referrer-Policy", "strict-origin-when-cross-origin")
		c.Set("Cache-Control", "no-store")
		c.Set("Content-Security-Policy", "default-src 'self'")
		c.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		return c.Next()
	}
}

// RequestID injects a unique X-Request-ID header for every request (tracing).
func RequestID() fiber.Handler {
	return func(c *fiber.Ctx) error {
		rid := c.Get("X-Request-ID")
		if rid == "" {
			rid = uuid.New().String()
		}
		c.Set("X-Request-ID", rid)
		c.Locals("request_id", rid)
		return c.Next()
	}
}

// CORS handles Cross-Origin Resource Sharing for dashboard/API access.
// Allows any origin for VAST serving but restricts admin APIs.
func CORS() fiber.Handler {
	allowedOrigin := os.Getenv("SSP_CORS_ORIGIN")
	return func(c *fiber.Ctx) error {
		origin := c.Get("Origin")
		if allowedOrigin != "" && origin == allowedOrigin {
			c.Set("Access-Control-Allow-Origin", origin)
		} else {
			c.Set("Access-Control-Allow-Origin", "*")
		}
		c.Set("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
		c.Set("Access-Control-Allow-Headers", "Content-Type,Authorization,X-Request-ID")
		c.Set("Access-Control-Max-Age", "86400")

		if c.Method() == fiber.MethodOptions {
			return c.SendStatus(fiber.StatusNoContent)
		}
		return c.Next()
	}
}
