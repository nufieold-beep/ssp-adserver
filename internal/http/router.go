
package http

import (
	"github.com/gofiber/fiber/v2"
	"ssp/internal/auction"
	"ssp/internal/bidder"
	"ssp/internal/openrtb"
	"ssp/internal/vast"
)

func NewRouter() *fiber.App {

	app := fiber.New()

	manager := bidder.NewManager()

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

// In-memory campaign store
type Campaign struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}
var campaigns = map[int]*Campaign{
	1: {ID: 1, Name: "CTV Demo", Status: "active"},
	2: {ID: 2, Name: "InApp Test", Status: "paused"},
}
var nextCampaignID = 3

// List campaigns
app.Get("/api/campaigns", func(c *fiber.Ctx) error {
	var out []Campaign
	for _, camp := range campaigns {
		out = append(out, *camp)
	}
	return c.JSON(out)
})

// Get campaign by ID
app.Get("/api/campaigns/:id", func(c *fiber.Ctx) error {
	id, err := c.ParamsInt("id")
	if err != nil || id < 1 {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid campaign ID"})
	}
	camp, ok := campaigns[id]
	if !ok {
		return c.Status(404).JSON(fiber.Map{"error": "Campaign not found"})
	}
	return c.JSON(camp)
})

// Create campaign
app.Post("/api/campaigns", func(c *fiber.Ctx) error {
	var camp Campaign
	if err := c.BodyParser(&camp); err != nil || camp.Name == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}
	camp.ID = nextCampaignID
	nextCampaignID++
	if camp.Status == "" {
		camp.Status = "active"
	}
	campaigns[camp.ID] = &camp
	return c.Status(201).JSON(camp)
})

// Update campaign
app.Put("/api/campaigns/:id", func(c *fiber.Ctx) error {
	id, err := c.ParamsInt("id")
	if err != nil || id < 1 {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid campaign ID"})
	}
	camp, ok := campaigns[id]
	if !ok {
		return c.Status(404).JSON(fiber.Map{"error": "Campaign not found"})
	}
	var update Campaign
	if err := c.BodyParser(&update); err != nil || update.Name == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}
	camp.Name = update.Name
	camp.Status = update.Status
	return c.JSON(camp)
})

// Delete campaign
app.Delete("/api/campaigns/:id", func(c *fiber.Ctx) error {
	id, err := c.ParamsInt("id")
	if err != nil || id < 1 {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid campaign ID"})
	}
	if _, ok := campaigns[id]; !ok {
		return c.Status(404).JSON(fiber.Map{"error": "Campaign not found"})
	}
	delete(campaigns, id)
	return c.JSON(fiber.Map{"deleted": id})
})
// Campaign endpoints
app.Get("/api/campaigns", func(c *fiber.Ctx) error {
	// TODO: Return list of campaigns as JSON
	return c.JSON([]fiber.Map{
		{"id": 1, "name": "CTV Demo", "status": "active"},
		{"id": 2, "name": "InApp Test", "status": "paused"},
	})
})

app.Get("/api/campaigns/:id", func(c *fiber.Ctx) error {
	// TODO: Return campaign details by ID
	id := c.Params("id")
	return c.JSON(fiber.Map{"id": id, "name": "CTV Demo", "status": "active"})
})

app.Post("/api/campaigns", func(c *fiber.Ctx) error {
	// TODO: Create new campaign
	type Campaign struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	var camp Campaign
	if err := c.BodyParser(&camp); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}
	// Simulate creation
	return c.Status(201).JSON(fiber.Map{"id": 3, "name": camp.Name, "status": camp.Status})
})

app.Put("/api/campaigns/:id", func(c *fiber.Ctx) error {
	// TODO: Update campaign by ID
	id := c.Params("id")
	type Campaign struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	var camp Campaign
	if err := c.BodyParser(&camp); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}
	// Simulate update
	return c.JSON(fiber.Map{"id": id, "name": camp.Name, "status": camp.Status})
})

app.Delete("/api/campaigns/:id", func(c *fiber.Ctx) error {
	// TODO: Delete campaign by ID
	id := c.Params("id")
	return c.JSON(fiber.Map{"deleted": id})
})

	app.Get("/vast/:tag", func(c *fiber.Ctx) error {
		// Validate and parse OpenRTB request
		req := openrtb.BuildFromHTTP(c)
		if len(req.Imp) == 0 || req.Imp[0].Video == nil {
			return c.Status(400).JSON(fiber.Map{"error": "Video impression required"})
		}
		// Only allow CTV/in-app video
		if req.App == nil {
			return c.Status(400).JSON(fiber.Map{"error": "Only in-app/CTV requests allowed"})
		}
		// Call all bidders and handle errors
		bids := manager.CallAll(req)
		if len(bids) == 0 {
			return c.SendStatus(204)
		}
		winner := auction.SelectWinner(bids, req.Imp[0].BidFloor)
		if winner == nil {
			return c.SendStatus(204)
		}
		// Build VAST XML and handle errors
		xml := vast.Build(winner)
		if xml == "" {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to build VAST"})
		}
		return c.Type("xml").SendString(xml)
	})

	return app
}
