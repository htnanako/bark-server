package database

import (
	"fmt"
	"os"
)

type EnvBase struct {
}

func NewEnvBase() Database {
	return &EnvBase{}
}

func (d *EnvBase) CountAll() (int, error) {
	return 1, nil
}

func (d *EnvBase) CountByStatus(status string) (int, error) {
	if status == StatusActive {
		return 1, nil
	}
	return 0, nil
}

func (d *EnvBase) DeviceByKey(key string) (*Device, error) {
	if key == os.Getenv("BARK_KEY") {
		device := NewLegacyDevice(os.Getenv("BARK_KEY"), os.Getenv("BARK_DEVICE_TOKEN"))
		return device, nil
	}
	return nil, fmt.Errorf("key not found")
}

func (d *EnvBase) SaveDevice(device *Device) (string, error) {
	if device == nil {
		return "", fmt.Errorf("device is nil")
	}
	device.NormalizeDefaults()
	if device.Platform != LegacyIOSPlatform || device.ProviderID != LegacyIOSProviderID || device.AppID != LegacyIOSAppID || device.Topic != LegacyIOSTopic {
		return "", fmt.Errorf("serverless mode only supports legacy iOS devices")
	}
	if device.DeviceToken == os.Getenv("BARK_DEVICE_TOKEN") {
		return os.Getenv("BARK_KEY"), nil
	}
	return "", fmt.Errorf("device token is invalid")
}

func (d *EnvBase) DeleteDeviceByKey(key string) error {
	return fmt.Errorf("not supported")
}

func (d *EnvBase) SaveNotification(event *NotificationEvent) (int64, error) {
	return 0, fmt.Errorf("not supported")
}

func (d *EnvBase) NotificationsByDeviceSince(key string, afterID int64, limit int) ([]NotificationEvent, error) {
	return nil, fmt.Errorf("not supported")
}

func (d *EnvBase) Close() error {
	return nil
}
