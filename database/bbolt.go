package database

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

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
