package database

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lithammer/shortuuid/v3"
	"github.com/mritd/logger"
	"go.etcd.io/bbolt"
)

// BboltDB implement Database interface with ETCD's bbolt
type BboltDB struct {
}

var dbOnce sync.Once
var db *bbolt.DB

const (
	legacyBucketName = "device"
	deviceBucketName = "devices_v2"
	notificationBucketName = "notifications_v1"
	metaBucketName   = "meta"
	schemaVersionKey = "schema_version"
)

func NewBboltdb(dataDir string) Database {
	bboltSetup(dataDir)

	return &BboltDB{}
}

// CountAll Fetch records count
func (d *BboltDB) CountAll() (int, error) {
	var keypairCount int
	err := db.View(func(tx *bbolt.Tx) error {
		keypairCount = tx.Bucket([]byte(deviceBucketName)).Stats().KeyN
		return nil
	})

	if err != nil {
		return 0, err
	}

	return keypairCount, nil
}

// Close close the db file
func (d *BboltDB) Close() error {
	return db.Close()
}

// DeviceTokenByKey get device token of specified key
func (d *BboltDB) CountByStatus(status string) (int, error) {
	count := 0
	err := db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte(deviceBucketName)).ForEach(func(_, v []byte) error {
			var device Device
			if err := json.Unmarshal(v, &device); err != nil {
				return err
			}
			if device.Status == status {
				count++
			}
			return nil
		})
	})
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (d *BboltDB) DeviceByKey(key string) (*Device, error) {
	var device Device
	err := db.View(func(tx *bbolt.Tx) error {
		bs := tx.Bucket([]byte(deviceBucketName)).Get([]byte(key))
		if bs == nil {
			return fmt.Errorf("failed to get [%s] device from database", key)
		}
		return json.Unmarshal(bs, &device)
	})
	if err != nil {
		return nil, err
	}

	return &device, nil
}

// SaveDevice create or update device of specified key
func (d *BboltDB) SaveDevice(device *Device) (string, error) {
	device.NormalizeDefaults()
	err := db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(deviceBucketName))

		if device.DeviceKey != "" {
			if existing := bucket.Get([]byte(device.DeviceKey)); existing != nil {
				var existingDevice Device
				if err := json.Unmarshal(existing, &existingDevice); err != nil {
					return err
				}
				if device.CreatedAt.IsZero() {
					device.CreatedAt = existingDevice.CreatedAt
				}
				if device.StreamToken == "" {
					device.StreamToken = existingDevice.StreamToken
				}
			}
		}

		if device.DeviceKey == "" || bucket.Get([]byte(device.DeviceKey)) == nil {
			device.DeviceKey = shortuuid.New()
		}

		payload, err := json.Marshal(device)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(device.DeviceKey), payload)
	})

	if err != nil {
		return "", err
	}

	return device.DeviceKey, nil
}

func (d *BboltDB) SaveNotification(event *NotificationEvent) (int64, error) {
	if event == nil {
		return 0, fmt.Errorf("notification event is nil")
	}
	if event.DeviceKey == "" {
		return 0, fmt.Errorf("device key is empty")
	}
	if event.Event == "" {
		event.Event = "notification"
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = timeNowUTC()
	}

	var id int64
	err := db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(notificationBucketName))
		seq, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		event.ID = int64(seq)
		id = event.ID

		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}

		var key [8]byte
		binary.BigEndian.PutUint64(key[:], seq)
		return bucket.Put(key[:], payload)
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (d *BboltDB) NotificationsByDeviceSince(deviceKey string, afterID int64, limit int) ([]NotificationEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	events := make([]NotificationEvent, 0, limit)
	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(notificationBucketName))
		cursor := bucket.Cursor()

		var startKey [8]byte
		binary.BigEndian.PutUint64(startKey[:], uint64(afterID+1))

		for k, v := cursor.Seek(startKey[:]); k != nil; k, v = cursor.Next() {
			var event NotificationEvent
			if err := json.Unmarshal(v, &event); err != nil {
				return err
			}
			if event.DeviceKey != deviceKey {
				continue
			}
			events = append(events, event)
			if len(events) >= limit {
				break
			}
		}
		return nil
	})
	return events, err
}

// DeleteDeviceByKey delete device of specified key
func (d *BboltDB) DeleteDeviceByKey(key string) error {
	err := db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(deviceBucketName))
		return bucket.Delete([]byte(key))
	})
	return err
}

// bboltSetup setup the bbolt database
func bboltSetup(dataDir string) {
	dbOnce.Do(func() {
		logger.Infof("init database [%s]...", dataDir)
		if _, err := os.Stat(dataDir); os.IsNotExist(err) {
			if err = os.MkdirAll(dataDir, 0755); err != nil {
				logger.Fatalf("failed to create database storage dir(%s): %v", dataDir, err)
			}
		} else if err != nil {
			logger.Fatalf("failed to open database storage dir(%s): %v", dataDir, err)
		}

		bboltDB, err := bbolt.Open(filepath.Join(dataDir, "bark.db"), 0600, nil)
		if err != nil {
			logger.Fatalf("failed to create database file(%s): %v", filepath.Join(dataDir, "bark.db"), err)
		}
		err = bboltDB.Update(func(tx *bbolt.Tx) error {
			if _, err := tx.CreateBucketIfNotExists([]byte(legacyBucketName)); err != nil {
				return err
			}
			if _, err := tx.CreateBucketIfNotExists([]byte(deviceBucketName)); err != nil {
				return err
			}
			if _, err := tx.CreateBucketIfNotExists([]byte(notificationBucketName)); err != nil {
				return err
			}
			if _, err := tx.CreateBucketIfNotExists([]byte(metaBucketName)); err != nil {
				return err
			}
			return migrateLegacyBucket(tx)
		})
		if err != nil {
			logger.Fatalf("failed to create database bucket: %v", err)
		}
		db = bboltDB
	})
}

func timeNowUTC() time.Time { return time.Now().UTC() }

func migrateLegacyBucket(tx *bbolt.Tx) error {
	metaBucket := tx.Bucket([]byte(metaBucketName))
	if string(metaBucket.Get([]byte(schemaVersionKey))) == fmt.Sprintf("%d", CurrentSchemaVersion) {
		return nil
	}

	deviceBucket := tx.Bucket([]byte(deviceBucketName))
	legacyBucket := tx.Bucket([]byte(legacyBucketName))
	err := legacyBucket.ForEach(func(k, v []byte) error {
		if len(k) == 0 || len(v) == 0 {
			return nil
		}
		if deviceBucket.Get(k) != nil {
			return nil
		}
		device := NewLegacyDevice(string(k), string(v))
		payload, err := json.Marshal(device)
		if err != nil {
			return err
		}
		return deviceBucket.Put(k, payload)
	})
	if err != nil {
		return err
	}
	return metaBucket.Put([]byte(schemaVersionKey), []byte(fmt.Sprintf("%d", CurrentSchemaVersion)))
}
