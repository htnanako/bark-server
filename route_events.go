package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/finb/bark-server/v2/database"
	"github.com/gofiber/fiber/v2"
)

const (
	defaultReplayLimit     = 200
	defaultHeartbeatPeriod = 25 * time.Second
)

func init() {
	registerRoute("events", func(router fiber.Router) {
		router.Get("/events/:device_key", streamDeviceEvents)
	})
}

func streamDeviceEvents(c *fiber.Ctx) error {
	deviceKey := strings.TrimSpace(c.Params("device_key"))
	if deviceKey == "" {
		return c.Status(fiber.StatusBadRequest).JSON(failed(400, "device key is empty"))
	}

	device, err := db.DeviceByKey(deviceKey)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(failed(404, "failed to get device: %v", err))
	}
	if err := authorizeEventStream(c, device); err != nil {
		return err
	}

	lastEventID := parseLastEventID(c)
	replayEvents, err := db.NotificationsByDeviceSince(deviceKey, lastEventID, defaultReplayLimit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(failed(500, "failed to load notification history: %v", err))
	}

	c.Set(fiber.HeaderContentType, "text/event-stream")
	c.Set(fiber.HeaderCacheControl, "no-cache, no-transform")
	c.Set(fiber.HeaderConnection, "keep-alive")
	c.Set("X-Accel-Buffering", "no")

	sub := notificationHub.Subscribe(deviceKey)
	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		defer notificationHub.Unsubscribe(sub)

		if err := writeSSE(w, "ready", 0, map[string]interface{}{
			"device_key": deviceKey,
			"replayed":   len(replayEvents),
		}); err != nil {
			return
		}

		for _, event := range replayEvents {
			if err := writeSSE(w, event.Event, event.ID, event); err != nil {
				return
			}
		}

		heartbeat := time.NewTicker(defaultHeartbeatPeriod)
		defer heartbeat.Stop()

		for {
			select {
			case event, ok := <-sub.events:
				if !ok {
					return
				}
				if err := writeSSE(w, event.Event, event.ID, event); err != nil {
					return
				}
			case <-heartbeat.C:
				if _, err := w.WriteString(": ping\n\n"); err != nil {
					return
				}
				if err := w.Flush(); err != nil {
					return
				}
			}
		}
	})
	return nil
}

func authorizeEventStream(c *fiber.Ctx, device *database.Device) error {
	token := strings.TrimSpace(c.Get(fiber.HeaderAuthorization))
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[7:])
	}
	if token == "" {
		token = strings.TrimSpace(c.Query("stream_token"))
	}
	if token == "" || token != device.StreamToken {
		return c.Status(fiber.StatusUnauthorized).JSON(failed(401, "invalid stream token"))
	}
	return nil
}

func parseLastEventID(c *fiber.Ctx) int64 {
	candidates := []string{
		c.Get("Last-Event-ID"),
		c.Query("last_event_id"),
		c.Query("since_id"),
	}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if value, err := strconv.ParseInt(candidate, 10, 64); err == nil && value >= 0 {
			return value
		}
	}
	return 0
}

func writeSSE(w *bufio.Writer, eventName string, id int64, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if id > 0 {
		if _, err := fmt.Fprintf(w, "id: %d\n", id); err != nil {
			return err
		}
	}
	if eventName != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", eventName); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return err
	}
	return w.Flush()
}
