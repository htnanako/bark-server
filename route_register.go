package main

import (
	"errors"
	"net/http"
	"strings"

	"github.com/finb/bark-server/v2/database"
	"github.com/gofiber/fiber/v2"
	"github.com/mritd/logger"
)

var errDeviceIdentityConflict = errors.New("device key already belongs to another registered app/platform")

type DeviceInfo struct {
	DeviceKey   string `form:"device_key,omitempty" json:"device_key,omitempty" xml:"device_key,omitempty" query:"device_key,omitempty"`
	DeviceToken string `form:"device_token,omitempty" json:"device_token,omitempty" xml:"device_token,omitempty" query:"device_token,omitempty"`
	Platform    string `form:"platform,omitempty" json:"platform,omitempty" xml:"platform,omitempty" query:"platform,omitempty"`
	AppID       string `form:"app_id,omitempty" json:"app_id,omitempty" xml:"app_id,omitempty" query:"app_id,omitempty"`
	ProviderID  string `form:"provider_id,omitempty" json:"provider_id,omitempty" xml:"provider_id,omitempty" query:"provider_id,omitempty"`
	Topic       string `form:"topic,omitempty" json:"topic,omitempty" xml:"topic,omitempty" query:"topic,omitempty"`

	// compatible with old req
	OldDeviceKey   string `form:"key,omitempty" json:"key,omitempty" xml:"key,omitempty" query:"key,omitempty"`
	OldDeviceToken string `form:"devicetoken,omitempty" json:"devicetoken,omitempty" xml:"devicetoken,omitempty" query:"devicetoken,omitempty"`
}

func init() {
	registerRoute("register", func(router fiber.Router) {
		router.Post("/register", func(c *fiber.Ctx) error { return doRegister(c, false) })
		router.Get("/register/:device_key", doRegisterCheck)
	})

	// compatible with old requests
	registerRouteWithWeight("register_compat", 100, func(router fiber.Router) {
		router.Get("/register", func(c *fiber.Ctx) error { return doRegister(c, true) })
	})
}

func doRegister(c *fiber.Ctx, compat bool) error {
	var deviceInfo DeviceInfo
	if compat {
		if err := c.QueryParser(&deviceInfo); err != nil {
			return c.Status(400).JSON(failed(400, "request bind failed1: %v", err))
		}
	} else {
		if err := c.BodyParser(&deviceInfo); err != nil {
			return c.Status(400).JSON(failed(400, "request bind failed2: %v", err))
		}
	}

	if deviceInfo.DeviceKey == "" && deviceInfo.OldDeviceKey != "" {
		deviceInfo.DeviceKey = deviceInfo.OldDeviceKey
	}

	// if deviceInfo.DeviceKey=="", newKey will be filled with a new uuid
	// otherwise it equal to deviceInfo.DeviceKey
	normalizeLegacyFields(&deviceInfo)
	device, err := buildRegisteredDevice(&deviceInfo, compat)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errDeviceIdentityConflict) {
			status = fiber.StatusConflict
		}
		return c.Status(status).JSON(failed(status, "%s", err.Error()))
	}
	newKey, err := db.SaveDevice(device)
	if err != nil {
		logger.Errorf("device registration failed: %v", err)
		return c.Status(500).JSON(failed(500, "device registration failed: %v", err))
	}
	device.DeviceKey = newKey

	return c.Status(200).JSON(data(map[string]string{
		// compatible with old resp
		"key":          device.DeviceKey,
		"device_key":   device.DeviceKey,
		"device_token": device.DeviceToken,
		"stream_token": device.StreamToken,
		"platform":     device.Platform,
		"app_id":       device.AppID,
		"provider_id":  device.ProviderID,
	}))
}

func doRegisterCheck(c *fiber.Ctx) error {
	deviceKey := c.Params("device_key")

	if deviceKey == "" {
		return c.Status(400).JSON(failed(400, "device key is empty"))
	}

	_, err := db.DeviceByKey(deviceKey)
	if err != nil {
		return c.Status(400).JSON(failed(400, "%s", err.Error()))
	}
	return c.Status(200).JSON(success())
}

func normalizeLegacyFields(deviceInfo *DeviceInfo) {
	if deviceInfo.DeviceKey == "" && deviceInfo.OldDeviceKey != "" {
		deviceInfo.DeviceKey = deviceInfo.OldDeviceKey
	}

	if deviceInfo.DeviceToken == "" && deviceInfo.OldDeviceToken != "" {
		deviceInfo.DeviceToken = deviceInfo.OldDeviceToken
	}
	deviceInfo.Platform = strings.ToLower(strings.TrimSpace(deviceInfo.Platform))
	deviceInfo.AppID = strings.TrimSpace(deviceInfo.AppID)
	deviceInfo.ProviderID = strings.TrimSpace(deviceInfo.ProviderID)
	deviceInfo.Topic = strings.TrimSpace(deviceInfo.Topic)
}

func buildRegisteredDevice(deviceInfo *DeviceInfo, compat bool) (*database.Device, error) {
	isLegacyCompat := compat || isLegacyRegistration(deviceInfo)
	if isLegacyCompat {
		deviceInfo.Platform = database.LegacyIOSPlatform
		deviceInfo.AppID = database.LegacyIOSAppID
		deviceInfo.ProviderID = database.LegacyIOSProviderID
		deviceInfo.Topic = database.LegacyIOSTopic
	}

	resolvedProvider, err := providerRegistry.ResolveRegistration(deviceInfo.Platform, deviceInfo.AppID, deviceInfo.ProviderID)
	if err != nil {
		return nil, err
	}
	deviceInfo.Platform = resolvedProvider.Config.Platform
	deviceInfo.AppID = resolvedProvider.Config.AppID
	deviceInfo.ProviderID = resolvedProvider.Config.ProviderID
	if deviceInfo.Topic == "" {
		deviceInfo.Topic = resolvedProvider.Config.Topic
	}

	device := &database.Device{
		DeviceKey:   deviceInfo.DeviceKey,
		DeviceToken: deviceInfo.DeviceToken,
		Platform:    deviceInfo.Platform,
		AppID:       deviceInfo.AppID,
		ProviderID:  deviceInfo.ProviderID,
		Topic:       deviceInfo.Topic,
		Status:      database.StatusActive,
	}

	if err := resolvedProvider.Provider.ValidateRegistration(device); err != nil {
		return nil, err
	}

	if device.DeviceKey == "" {
		return device, nil
	}

	existingDevice, err := db.DeviceByKey(device.DeviceKey)
	if err != nil {
		return device, nil
	}
	if !sameDeviceIdentity(existingDevice, device) {
		return nil, errDeviceIdentityConflict
	}
	device.CreatedAt = existingDevice.CreatedAt
	device.StreamToken = existingDevice.StreamToken
	return device, nil
}

func isLegacyRegistration(deviceInfo *DeviceInfo) bool {
	return deviceInfo.Platform == "" && deviceInfo.AppID == "" && deviceInfo.ProviderID == "" && deviceInfo.Topic == ""
}

func sameDeviceIdentity(a, b *database.Device) bool {
	return a.Platform == b.Platform && a.AppID == b.AppID && a.ProviderID == b.ProviderID && a.Topic == b.Topic
}
