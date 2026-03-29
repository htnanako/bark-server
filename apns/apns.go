package apns

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/mritd/logger"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/payload"
	"github.com/sideshow/apns2/token"
	"golang.org/x/net/http2"
)

const (
	LegacyTopic  = "me.fin.bark"
	LegacyKeyID  = "LH4T9V5U4R"
	LegacyTeamID = "5U8LBRXG3A"
)

type PushMessage struct {
	Id          string `form:"id,omitempty" json:"id,omitempty" xml:"id,omitempty" query:"id,omitempty"`
	DeviceToken string `form:"-" json:"-" xml:"-" query:"-"`
	DeviceKey   string `form:"device_key,omitempty" json:"device_key,omitempty" xml:"device_key,omitempty" query:"device_key,omitempty"`
	Subtitle    string `form:"subtitle,omitempty" json:"subtitle,omitempty" xml:"subtitle,omitempty" query:"subtitle,omitempty"`
	Title       string `form:"title,omitempty" json:"title,omitempty" xml:"title,omitempty" query:"title,omitempty"`
	Body        string `form:"body,omitempty" json:"body,omitempty" xml:"body,omitempty" query:"body,omitempty"`
	// ios notification sound(system sound please refer to http://iphonedevwiki.net/index.php/AudioServices)
	Sound     string                 `form:"sound,omitempty" json:"sound,omitempty" xml:"sound,omitempty" query:"sound,omitempty"`
	ExtParams map[string]interface{} `form:"ext_params,omitempty" json:"ext_params,omitempty" xml:"ext_params,omitempty" query:"ext_params,omitempty"`
}

// Check if it's an empty message, empty messages might be silent push notifications
func (p PushMessage) IsEmptyAlert() bool {
	return p.Title == "" && p.Body == "" && p.Subtitle == ""
}

func (p PushMessage) IsDelete() bool {
	val := p.ExtParams["delete"]
	return val == "1" || val == 1 || val == 1.0
}

const PayloadMaximum = 4096

type Config struct {
	Topic          string
	KeyID          string
	TeamID         string
	PrivateKey     string
	Environment    string
	MaxClientCount int
}

type Client struct {
	topic   string
	clients chan *apns2.Client
}

var legacyClient *Client

// Initialize APNS client pool
func init() {
	ReCreateAPNS(1)
}

func LegacyConfig(maxClientCount int) Config {
	return Config{
		Topic:          LegacyTopic,
		KeyID:          LegacyKeyID,
		TeamID:         LegacyTeamID,
		PrivateKey:     apnsPrivateKey,
		Environment:    "production",
		MaxClientCount: maxClientCount,
	}
}

func ReCreateAPNS(maxClientCount int) error {
	client, err := NewClient(LegacyConfig(maxClientCount))
	if err != nil {
		return err
	}
	legacyClient = client
	return nil
}

func NewClient(cfg Config) (*Client, error) {
	maxClientCount := cfg.MaxClientCount
	if maxClientCount < 1 {
		maxClientCount = 1
	}
	if cfg.Topic == "" || cfg.KeyID == "" || cfg.TeamID == "" || cfg.PrivateKey == "" {
		return nil, fmt.Errorf("invalid apns config")
	}

	authKey, err := token.AuthKeyFromBytes([]byte(cfg.PrivateKey))
	if err != nil {
		return nil, fmt.Errorf("failed to create APNS auth key: %w", err)
	}

	var rootCAs *x509.CertPool
	if runtime.GOOS == "windows" {
		rootCAs = x509.NewCertPool()
	} else {
		rootCAs, err = x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("failed to get rootCAs: %w", err)
		}
	}

	for _, ca := range apnsCAs {
		rootCAs.AppendCertsFromPEM([]byte(ca))
	}

	host := apns2.HostProduction
	if strings.EqualFold(cfg.Environment, "development") {
		host = apns2.HostDevelopment
	}

	clients := make(chan *apns2.Client, maxClientCount)

	for i := 0; i < min(runtime.NumCPU(), maxClientCount); i++ {
		client := &apns2.Client{
			Token: &token.Token{
				AuthKey: authKey,
				KeyID:   cfg.KeyID,
				TeamID:  cfg.TeamID,
			},
			HTTPClient: &http.Client{
				Transport: &http2.Transport{
					DialTLS: apns2.DialTLS,
					TLSClientConfig: &tls.Config{
						RootCAs: rootCAs,
					},
				},
				Timeout: apns2.HTTPClientTimeout,
			},
			Host: host,
		}
		logger.Infof("create apns client: %d", i)
		clients <- client
	}

	logger.Info("init apns client success...")
	return &Client{
		topic:   cfg.Topic,
		clients: clients,
	}, nil
}

func Push(msg *PushMessage) (code int, err error) {
	if legacyClient == nil {
		return 500, fmt.Errorf("legacy apns client not initialized")
	}
	return legacyClient.Push(msg)
}

func (c *Client) Push(msg *PushMessage) (code int, err error) {
	if c == nil {
		return 500, fmt.Errorf("apns client is nil")
	}
	pl := payload.NewPayload().MutableContent()
	pushType := apns2.PushTypeAlert
	if msg.IsDelete() {
		// Silent push notification
		pl = pl.ContentAvailable()
		pushType = apns2.PushTypeBackground
	} else {
		// Regular push notification
		pl = pl.AlertTitle(msg.Title).
			AlertSubtitle(msg.Subtitle).
			AlertBody(msg.Body).
			Sound(msg.Sound).
			Category("myNotificationCategory")
		group, exist := msg.ExtParams["group"]
		if exist {
			pl = pl.ThreadID(group.(string))
		}
	}

	for k, v := range msg.ExtParams {
		// Change all parameter names to lowercase to prevent inconsistent capitalization
		pl.Custom(strings.ToLower(k), fmt.Sprintf("%v", v))
	}

	client := <-c.clients // grab a client from the pool
	c.clients <- client   // add the client back to the pool

	resp, err := client.Push(&apns2.Notification{
		CollapseID:  msg.Id,
		DeviceToken: msg.DeviceToken,
		Topic:       c.topic,
		Payload:     pl,
		Expiration:  time.Now().Add(24 * time.Hour),
		PushType:    pushType,
	})
	if err != nil {
		return 500, err
	}
	if resp.StatusCode != 200 {
		return resp.StatusCode, fmt.Errorf(resp.Reason)
	}
	return 200, nil
}
