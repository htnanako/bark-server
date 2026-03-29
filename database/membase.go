package database

import (
	"fmt"
	"sync"
)

type MemBase struct {
	mu            sync.RWMutex
	devices       map[string]*Device
	notifications []NotificationEvent
	nextEventID   int64
}

func NewMemBase() Database {
	return &MemBase{
		devices:       map[string]*Device{},
		notifications: make([]NotificationEvent, 0),
	}
}

func (d *MemBase) CountAll() (int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.devices), nil
}

func (d *MemBase) CountByStatus(status string) (int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	count := 0
	for _, device := range d.devices {
		if device.Status == status {
			count++
		}
	}
	return count, nil
}

func (d *MemBase) DeviceByKey(key string) (*Device, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	device, ok := d.devices[key]
	if !ok {
		return nil, fmt.Errorf("key not found")
	}
	copy := *device
	return &copy, nil
}

func (d *MemBase) SaveDevice(device *Device) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	device.NormalizeDefaults()
	if device.DeviceKey == "" {
		device.DeviceKey = "MemoryBaseKey"
		if _, exists := d.devices[device.DeviceKey]; exists {
			device.DeviceKey = fmt.Sprintf("MemoryBaseKey-%d", len(d.devices)+1)
		}
	}
	copy := *device
	d.devices[device.DeviceKey] = &copy
	return device.DeviceKey, nil
}

func (d *MemBase) DeleteDeviceByKey(key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.devices[key]; !ok {
		return fmt.Errorf("key not found")
	}
	delete(d.devices, key)
	return nil
}

func (d *MemBase) SaveNotification(event *NotificationEvent) (int64, error) {
	if event == nil {
		return 0, fmt.Errorf("notification event is nil")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nextEventID++
	copy := *event
	copy.ID = d.nextEventID
	d.notifications = append(d.notifications, copy)
	return copy.ID, nil
}

func (d *MemBase) NotificationsByDeviceSince(key string, afterID int64, limit int) ([]NotificationEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	events := make([]NotificationEvent, 0, limit)
	for _, item := range d.notifications {
		if item.DeviceKey != key || item.ID <= afterID {
			continue
		}
		events = append(events, item)
		if len(events) >= limit {
			break
		}
	}
	return events, nil
}

func (d *MemBase) Close() error {
	return nil
}
