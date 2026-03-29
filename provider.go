package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/finb/bark-server/v2/apns"
	"github.com/finb/bark-server/v2/database"
	"gopkg.in/yaml.v3"
)

const legacyProviderID = database.LegacyIOSProviderID

const (
	defaultMacOSSSEProviderID = "macos_sse"
	defaultMacOSAppID         = "me.fin.bark.macos"
)

type ProviderConfig struct {
	ProviderID  string `json:"provider_id" yaml:"provider_id"`
	Delivery    string `json:"delivery" yaml:"delivery"`
	Platform    string `json:"platform" yaml:"platform"`
	AppID       string `json:"app_id" yaml:"app_id"`
	Topic       string `json:"topic" yaml:"topic"`
	TeamID      string `json:"team_id" yaml:"team_id"`
	KeyID       string `json:"key_id" yaml:"key_id"`
	PrivateKey  string `json:"private_key" yaml:"private_key"`
	Environment string `json:"environment" yaml:"environment"`
}

type providersFile struct {
	Providers []ProviderConfig `json:"providers" yaml:"providers"`
}

type PushProvider interface {
	ID() string
	ValidateRegistration(device *database.Device) error
	Send(msg *apns.PushMessage, device *database.Device) (int, error)
}

type registeredProvider struct {
	Config   ProviderConfig
	Provider PushProvider
}

type ProviderRegistry struct {
	byID              map[string]registeredProvider
	defaultByPlatform map[string]string
}

type APNSProvider struct {
	config ProviderConfig
	client *apns.Client
}

type SSEProvider struct {
	config ProviderConfig
}

var (
	providerRegistry   *ProviderRegistry
	maxAPNSClientCount = 1
)

func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{
		byID:              map[string]registeredProvider{},
		defaultByPlatform: map[string]string{},
	}
}

func SetMaxAPNSClientCount(count int) {
	if count > 0 {
		maxAPNSClientCount = count
	}
}

func initializeProviders(configPath string) error {
	registry := NewProviderRegistry()
	cfgs := []ProviderConfig{}

	legacy := legacyProviderConfig()
	if err := registry.AddAPNSProvider(legacy); err != nil {
		return err
	}

	if configPath != "" {
		loaded, err := loadProviderConfigs(configPath)
		if err != nil {
			return err
		}
		cfgs = loaded
	}

	if !hasProviderID(cfgs, defaultMacOSSSEProviderID) {
		if err := registry.AddSSEProvider(defaultMacOSSSEProviderConfig()); err != nil {
			return err
		}
	}

	for _, cfg := range cfgs {
		if strings.EqualFold(strings.TrimSpace(cfg.Delivery), "sse") {
			if err := registry.AddSSEProvider(cfg); err != nil {
				return err
			}
			continue
		}
		if err := registry.AddAPNSProvider(cfg); err != nil {
			return err
		}
	}

	providerRegistry = registry
	return nil
}

func loadProviderConfigs(path string) ([]ProviderConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read providers config: %w", err)
	}
	var file providersFile
	if err := json.Unmarshal(data, &file); err != nil {
		if yamlErr := yaml.Unmarshal(data, &file); yamlErr != nil {
			return nil, fmt.Errorf("parse providers config: %w", err)
		}
	}
	return file.Providers, nil
}

func legacyProviderConfig() ProviderConfig {
	cfg := apns.LegacyConfig(maxAPNSClientCount)
	return ProviderConfig{
		ProviderID:  legacyProviderID,
		Platform:    database.LegacyIOSPlatform,
		AppID:       database.LegacyIOSAppID,
		Topic:       cfg.Topic,
		TeamID:      cfg.TeamID,
		KeyID:       cfg.KeyID,
		PrivateKey:  cfg.PrivateKey,
		Environment: cfg.Environment,
	}
}

func defaultMacOSSSEProviderConfig() ProviderConfig {
	return ProviderConfig{
		ProviderID: defaultMacOSSSEProviderID,
		Delivery:   "sse",
		Platform:   "macos",
		AppID:      defaultMacOSAppID,
		Topic:      defaultMacOSAppID,
	}
}

func hasProviderID(cfgs []ProviderConfig, providerID string) bool {
	for _, cfg := range cfgs {
		if strings.EqualFold(strings.TrimSpace(cfg.ProviderID), providerID) {
			return true
		}
	}
	return false
}

func (r *ProviderRegistry) AddAPNSProvider(cfg ProviderConfig) error {
	cfg = normalizeProviderConfig(cfg)
	if cfg.ProviderID == "" || cfg.Platform == "" || cfg.AppID == "" || cfg.Topic == "" {
		return fmt.Errorf("provider config is incomplete")
	}
	if _, exists := r.byID[cfg.ProviderID]; exists {
		return fmt.Errorf("duplicate provider_id: %s", cfg.ProviderID)
	}

	client, err := apns.NewClient(apns.Config{
		Topic:          cfg.Topic,
		KeyID:          cfg.KeyID,
		TeamID:         cfg.TeamID,
		PrivateKey:     cfg.PrivateKey,
		Environment:    cfg.Environment,
		MaxClientCount: maxAPNSClientCount,
	})
	if err != nil {
		return fmt.Errorf("initialize provider %s: %w", cfg.ProviderID, err)
	}
	provider := &APNSProvider{
		config: cfg,
		client: client,
	}
	return r.AddProvider(cfg, provider)
}

func (r *ProviderRegistry) AddProvider(cfg ProviderConfig, provider PushProvider) error {
	cfg = normalizeProviderConfig(cfg)
	if cfg.ProviderID == "" || cfg.Platform == "" || cfg.AppID == "" || cfg.Topic == "" {
		return fmt.Errorf("provider config is incomplete")
	}
	if _, exists := r.byID[cfg.ProviderID]; exists {
		return fmt.Errorf("duplicate provider_id: %s", cfg.ProviderID)
	}
	platformKey := defaultProviderKey(cfg.Platform, cfg.AppID)
	r.byID[cfg.ProviderID] = registeredProvider{
		Config:   cfg,
		Provider: provider,
	}
	if _, exists := r.defaultByPlatform[platformKey]; !exists {
		r.defaultByPlatform[platformKey] = cfg.ProviderID
	}
	return nil
}

func normalizeProviderConfig(cfg ProviderConfig) ProviderConfig {
	cfg.ProviderID = strings.TrimSpace(cfg.ProviderID)
	cfg.Delivery = strings.ToLower(strings.TrimSpace(cfg.Delivery))
	cfg.Platform = strings.ToLower(strings.TrimSpace(cfg.Platform))
	cfg.AppID = strings.TrimSpace(cfg.AppID)
	cfg.Topic = strings.TrimSpace(cfg.Topic)
	cfg.TeamID = strings.TrimSpace(cfg.TeamID)
	cfg.KeyID = strings.TrimSpace(cfg.KeyID)
	cfg.PrivateKey = strings.TrimSpace(cfg.PrivateKey)
	if cfg.Delivery == "" {
		cfg.Delivery = "apns"
	}
	cfg.Environment = strings.ToLower(strings.TrimSpace(cfg.Environment))
	if cfg.Environment == "" {
		cfg.Environment = "production"
	}
	return cfg
}

func (r *ProviderRegistry) AddSSEProvider(cfg ProviderConfig) error {
	cfg = normalizeProviderConfig(cfg)
	provider := &SSEProvider{config: cfg}
	return r.AddProvider(cfg, provider)
}

func defaultProviderKey(platform, appID string) string {
	return strings.ToLower(strings.TrimSpace(platform)) + "|" + strings.TrimSpace(appID)
}

func (r *ProviderRegistry) ResolveRegistration(platform, appID, providerID string) (registeredProvider, error) {
	if r == nil {
		return registeredProvider{}, fmt.Errorf("provider registry is not initialized")
	}
	if providerID != "" {
		provider, ok := r.byID[providerID]
		if !ok {
			return registeredProvider{}, fmt.Errorf("provider not found: %s", providerID)
		}
		return provider, nil
	}
	key := defaultProviderKey(platform, appID)
	id, ok := r.defaultByPlatform[key]
	if !ok {
		return registeredProvider{}, fmt.Errorf("default provider not found for %s/%s", platform, appID)
	}
	return r.byID[id], nil
}

func (r *ProviderRegistry) ProviderForDevice(device *database.Device) (registeredProvider, error) {
	if r == nil {
		return registeredProvider{}, fmt.Errorf("provider registry is not initialized")
	}
	provider, ok := r.byID[device.ProviderID]
	if !ok {
		return registeredProvider{}, fmt.Errorf("provider not found: %s", device.ProviderID)
	}
	return provider, nil
}

func (r *ProviderRegistry) IDs() []string {
	if r == nil {
		return nil
	}
	ids := make([]string, 0, len(r.byID))
	for id := range r.byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (r *ProviderRegistry) Count() int {
	if r == nil {
		return 0
	}
	return len(r.byID)
}

func (p *APNSProvider) ID() string {
	return p.config.ProviderID
}

func (p *APNSProvider) ValidateRegistration(device *database.Device) error {
	if device.DeviceToken == "" {
		return fmt.Errorf("device token is empty")
	}
	if len(device.DeviceToken) > 160 {
		return fmt.Errorf("device token is invalid")
	}
	if device.Platform != p.config.Platform {
		return fmt.Errorf("platform does not match provider")
	}
	if device.AppID != p.config.AppID {
		return fmt.Errorf("app_id does not match provider")
	}
	if device.Topic != p.config.Topic {
		return fmt.Errorf("topic does not match provider")
	}
	return nil
}

func (p *APNSProvider) Send(msg *apns.PushMessage, device *database.Device) (int, error) {
	msg.DeviceToken = device.DeviceToken
	return p.client.Push(msg)
}

func (p *SSEProvider) ID() string {
	return p.config.ProviderID
}

func (p *SSEProvider) ValidateRegistration(device *database.Device) error {
	if device.Platform != p.config.Platform {
		return fmt.Errorf("platform does not match provider")
	}
	if device.AppID != p.config.AppID {
		return fmt.Errorf("app_id does not match provider")
	}
	if device.Topic != p.config.Topic {
		return fmt.Errorf("topic does not match provider")
	}
	return nil
}

func (p *SSEProvider) Send(msg *apns.PushMessage, device *database.Device) (int, error) {
	event := database.NotificationEvent{
		DeviceKey: device.DeviceKey,
		Event:     "notification",
		Title:     msg.Title,
		Subtitle:  msg.Subtitle,
		Body:      msg.Body,
		Payload:   clonePayload(msg),
	}
	id, err := db.SaveNotification(&event)
	if err != nil {
		return 500, err
	}
	event.ID = id
	notificationHub.Publish(event)
	return 200, nil
}

func clonePayload(msg *apns.PushMessage) map[string]interface{} {
	payload := make(map[string]interface{}, len(msg.ExtParams)+6)
	payload["id"] = msg.Id
	payload["device_key"] = msg.DeviceKey
	payload["title"] = msg.Title
	payload["subtitle"] = msg.Subtitle
	payload["body"] = msg.Body
	payload["sound"] = msg.Sound
	for k, v := range msg.ExtParams {
		payload[k] = v
	}
	return payload
}
