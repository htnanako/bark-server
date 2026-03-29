package database

import (
	"time"

	"github.com/lithammer/shortuuid/v3"
)

type NotificationEvent struct {
	ID        int64                  `json:"id" db:"id"`
	DeviceKey string                 `json:"device_key" db:"device_key"`
	Event     string                 `json:"event" db:"event"`
	Title     string                 `json:"title,omitempty" db:"title"`
	Subtitle  string                 `json:"subtitle,omitempty" db:"subtitle"`
	Body      string                 `json:"body,omitempty" db:"body"`
	Payload   map[string]interface{} `json:"payload,omitempty" db:"payload"`
	CreatedAt time.Time              `json:"created_at" db:"created_at"`
}

func NewStreamToken() string {
	return shortuuid.New()
}
