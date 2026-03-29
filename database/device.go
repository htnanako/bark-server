package database

import "time"

const (
	CurrentSchemaVersion = 2

	StatusActive  = "active"
	StatusInvalid = "invalid"

	LegacyIOSPlatform   = "ios"
	LegacyIOSAppID      = "me.fin.bark"
	LegacyIOSProviderID = "ios_legacy"
	LegacyIOSTopic      = "me.fin.bark"
)

type Device struct {
	DeviceKey        string    `json:"device_key" db:"key"`
	DeviceToken      string    `json:"device_token" db:"token"`
	Platform         string    `json:"platform" db:"platform"`
	AppID            string    `json:"app_id" db:"app_id"`
	ProviderID       string    `json:"provider_id" db:"provider_id"`
	Topic            string    `json:"topic" db:"topic"`
	Status           string    `json:"status" db:"status"`
	CreatedAt        time.Time `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time `json:"updated_at" db:"updated_at"`
	LastRegisteredAt time.Time `json:"last_registered_at" db:"last_registered_at"`
}

func NewLegacyDevice(key, token string) *Device {
	now := time.Now().UTC()
	return &Device{
		DeviceKey:        key,
		DeviceToken:      token,
		Platform:         LegacyIOSPlatform,
		AppID:            LegacyIOSAppID,
		ProviderID:       LegacyIOSProviderID,
		Topic:            LegacyIOSTopic,
		Status:           StatusActive,
		CreatedAt:        now,
		UpdatedAt:        now,
		LastRegisteredAt: now,
	}
}

func (d *Device) NormalizeDefaults() {
	now := time.Now().UTC()
	if d.Platform == "" {
		d.Platform = LegacyIOSPlatform
	}
	if d.AppID == "" {
		d.AppID = LegacyIOSAppID
	}
	if d.ProviderID == "" {
		d.ProviderID = LegacyIOSProviderID
	}
	if d.Topic == "" {
		d.Topic = LegacyIOSTopic
	}
	if d.Status == "" {
		d.Status = StatusActive
	}
	if d.CreatedAt.IsZero() {
		d.CreatedAt = now
	}
	if d.LastRegisteredAt.IsZero() {
		d.LastRegisteredAt = now
	}
	d.UpdatedAt = now
}
