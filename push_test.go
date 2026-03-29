package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/finb/bark-server/v2/apns"
	"github.com/finb/bark-server/v2/database"
	"github.com/gofiber/fiber/v2"
	jsoniter "github.com/json-iterator/go"
)

const (
	legacyKey     = "MemoryBaseKey"
	legacyToken   = "legacy-token"
	macKey        = "mac-key"
	macToken      = "mac-token"
	badMacKey     = "bad-mac-key"
	badMacToken   = "bad-token"
	macProviderID = "macos_default"
	macPlatform   = "macos"
	macAppID      = "me.fin.bark.macos"
	macTopic      = "me.fin.bark.macos"
)

var (
	app            *fiber.App
	legacyProvider *mockProvider
	macProvider    *mockProvider
)

type mockProvider struct {
	id    string
	mu    sync.Mutex
	sends []mockSend
}

type mockSend struct {
	DeviceKey string
	Body      string
	Title     string
	Token     string
}

func (m *mockProvider) ID() string {
	return m.id
}

func (m *mockProvider) ValidateRegistration(device *database.Device) error {
	return nil
}

func (m *mockProvider) Send(msg *apns.PushMessage, device *database.Device) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sends = append(m.sends, mockSend{
		DeviceKey: device.DeviceKey,
		Body:      msg.Body,
		Title:     msg.Title,
		Token:     device.DeviceToken,
	})
	if device.DeviceToken == badMacToken {
		return 410, fiber.ErrGone
	}
	return 200, nil
}

func (m *mockProvider) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sends = nil
}

func (m *mockProvider) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sends)
}

func TestMain(m *testing.M) {
	db = database.NewMemBase()
	providerRegistry = NewProviderRegistry()

	legacyProvider = &mockProvider{id: database.LegacyIOSProviderID}
	macProvider = &mockProvider{id: macProviderID}

	mustAddProvider(ProviderConfig{
		ProviderID: database.LegacyIOSProviderID,
		Platform:   database.LegacyIOSPlatform,
		AppID:      database.LegacyIOSAppID,
		Topic:      database.LegacyIOSTopic,
	}, legacyProvider)
	mustAddProvider(ProviderConfig{
		ProviderID: macProviderID,
		Platform:   macPlatform,
		AppID:      macAppID,
		Topic:      macTopic,
	}, macProvider)

	_, _ = db.SaveDevice(database.NewLegacyDevice(legacyKey, legacyToken))
	_, _ = db.SaveDevice(&database.Device{
		DeviceKey:   macKey,
		DeviceToken: macToken,
		Platform:    macPlatform,
		AppID:       macAppID,
		ProviderID:  macProviderID,
		Topic:       macTopic,
		Status:      database.StatusActive,
	})
	_, _ = db.SaveDevice(&database.Device{
		DeviceKey:   badMacKey,
		DeviceToken: badMacToken,
		Platform:    macPlatform,
		AppID:       macAppID,
		ProviderID:  macProviderID,
		Topic:       macTopic,
		Status:      database.StatusActive,
	})

	app = NewServer()
	os.Exit(m.Run())
}

func TestRegisterLegacyCompat(t *testing.T) {
	legacyProvider.Reset()

	res := doJSONRequest(t, "GET", "/register?devicetoken=new-legacy-token", "", false)
	assertStatus(t, res, 200)

	payload := parseRespData(t, res)
	if payload["platform"] != database.LegacyIOSPlatform {
		t.Fatalf("expected legacy platform, got %#v", payload["platform"])
	}
	if payload["provider_id"] != database.LegacyIOSProviderID {
		t.Fatalf("expected legacy provider, got %#v", payload["provider_id"])
	}
}

func TestRegisterMacOSAndReregister(t *testing.T) {
	body := `{"device_token":"mac-new-token","platform":"macos","app_id":"me.fin.bark.macos"}`
	res := doJSONRequest(t, "POST", "/register", body, true)
	assertStatus(t, res, 200)

	payload := parseRespData(t, res)
	if payload["provider_id"] != macProviderID {
		t.Fatalf("expected mac provider id, got %#v", payload["provider_id"])
	}

	deviceKey, _ := payload["device_key"].(string)
	if deviceKey == "" {
		t.Fatal("expected device_key to be returned")
	}

	res = doJSONRequest(t, "POST", "/register", `{"device_key":"`+deviceKey+`","device_token":"mac-new-token-2","platform":"macos","app_id":"me.fin.bark.macos"}`, true)
	assertStatus(t, res, 200)
	_ = closeResponse(res)

	device, err := db.DeviceByKey(deviceKey)
	if err != nil {
		t.Fatal(err)
	}
	if device.DeviceToken != "mac-new-token-2" {
		t.Fatalf("expected device token to update, got %s", device.DeviceToken)
	}
}

func TestRegisterIdentityConflict(t *testing.T) {
	res := doJSONRequest(t, "POST", "/register", `{"device_key":"`+legacyKey+`","device_token":"mac-conflict","platform":"macos","app_id":"me.fin.bark.macos"}`, true)
	assertStatus(t, res, 409)
	_ = closeResponse(res)
}

func TestPushCompatAndV2(t *testing.T) {
	legacyProvider.Reset()
	macProvider.Reset()

	res := doJSONRequest(t, "GET", "/"+legacyKey+"/body", "", false)
	assertStatus(t, res, 200)
	_ = closeResponse(res)

	res = doJSONRequest(t, "POST", "/push", `{"device_key":"`+macKey+`","title":"hello","body":"mac-body"}`, true)
	assertStatus(t, res, 200)
	_ = closeResponse(res)

	res = doJSONRequest(t, "POST", "/push", `{"device_keys":["`+legacyKey+`","`+macKey+`"],"body":"fanout"}`, true)
	assertStatus(t, res, 200)
	_ = closeResponse(res)

	if legacyProvider.Count() != 2 {
		t.Fatalf("expected 2 legacy sends, got %d", legacyProvider.Count())
	}
	if macProvider.Count() != 2 {
		t.Fatalf("expected 2 mac sends, got %d", macProvider.Count())
	}
}

func TestInvalidTokenMarksDeviceInvalid(t *testing.T) {
	macProvider.Reset()

	res := doJSONRequest(t, "POST", "/push", `{"device_key":"`+badMacKey+`","body":"bad"}`, true)
	assertStatus(t, res, 500)
	_ = closeResponse(res)

	device, err := db.DeviceByKey(badMacKey)
	if err != nil {
		t.Fatal(err)
	}
	if device.Status != database.StatusInvalid {
		t.Fatalf("expected invalid status, got %s", device.Status)
	}

	res = doJSONRequest(t, "POST", "/push", `{"device_key":"`+badMacKey+`","body":"bad"}`, true)
	assertStatus(t, res, 400)
	_ = closeResponse(res)

	res = doJSONRequest(t, "POST", "/register", `{"device_key":"`+badMacKey+`","device_token":"recovered-token","platform":"macos","app_id":"me.fin.bark.macos"}`, true)
	assertStatus(t, res, 200)
	_ = closeResponse(res)

	res = doJSONRequest(t, "POST", "/push", `{"device_key":"`+badMacKey+`","body":"good"}`, true)
	assertStatus(t, res, 200)
	_ = closeResponse(res)
}

type CommonRespForTest struct {
	Code    int                    `json:"code"`
	Message string                 `json:"message"`
	Data    map[string]interface{} `json:"data"`
}

func NewServer() *fiber.App {
	fiberApp := fiber.New(fiber.Config{
		JSONEncoder: jsoniter.Marshal,
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			return c.Status(code).JSON(CommonResp{
				Code:      code,
				Message:   err.Error(),
				Timestamp: time.Now().Unix(),
			})
		},
	})

	routerSetup(fiberApp)
	return fiberApp
}

func mustAddProvider(cfg ProviderConfig, provider PushProvider) {
	if err := providerRegistry.AddProvider(cfg, provider); err != nil {
		panic(err)
	}
}

func doJSONRequest(t *testing.T, method, url, body string, isJSON bool) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	if isJSON {
		req.Header.Set("Content-Type", "application/json")
	} else {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	res, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func assertStatus(t *testing.T, res *http.Response, want int) {
	t.Helper()
	if res.StatusCode != want {
		body, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		t.Fatalf("want %d, got %d, body=%s", want, res.StatusCode, string(body))
	}
}

func parseRespData(t *testing.T, res *http.Response) map[string]interface{} {
	t.Helper()
	defer res.Body.Close()
	var payload CommonRespForTest
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	return payload.Data
}

func closeResponse(res *http.Response) error {
	if res == nil || res.Body == nil {
		return nil
	}
	return res.Body.Close()
}
