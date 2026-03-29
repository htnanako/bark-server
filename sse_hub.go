package main

import (
	"sync"

	"github.com/finb/bark-server/v2/database"
)

type deviceSubscription struct {
	deviceKey string
	events    chan database.NotificationEvent
}

type notificationEventHub struct {
	mu   sync.Mutex
	subs map[string]*deviceSubscription
}

var notificationHub = newNotificationEventHub()

func newNotificationEventHub() *notificationEventHub {
	return &notificationEventHub{
		subs: make(map[string]*deviceSubscription),
	}
}

func (h *notificationEventHub) Subscribe(deviceKey string) *deviceSubscription {
	h.mu.Lock()
	defer h.mu.Unlock()

	if existing, ok := h.subs[deviceKey]; ok {
		close(existing.events)
		delete(h.subs, deviceKey)
	}

	sub := &deviceSubscription{
		deviceKey: deviceKey,
		events:    make(chan database.NotificationEvent, 64),
	}
	h.subs[deviceKey] = sub
	return sub
}

func (h *notificationEventHub) Unsubscribe(sub *deviceSubscription) {
	if sub == nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	current, ok := h.subs[sub.deviceKey]
	if !ok || current != sub {
		return
	}
	close(sub.events)
	delete(h.subs, sub.deviceKey)
}

func (h *notificationEventHub) Publish(event database.NotificationEvent) {
	h.mu.Lock()
	sub, ok := h.subs[event.DeviceKey]
	h.mu.Unlock()
	if !ok {
		return
	}

	select {
	case sub.events <- event:
	default:
		h.Unsubscribe(sub)
	}
}
