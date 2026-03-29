package database

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/lithammer/shortuuid/v3"
	"github.com/mritd/logger"
)

type MySQL struct {
}

var mysqlDB *sql.DB

const (
	dbSchema = "" +
		"CREATE TABLE IF NOT EXISTS `devices` (" +
		"    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT," +
		"    `key` VARCHAR(255) NOT NULL," +
		"    `token` VARCHAR(255) NOT NULL," +
		"    PRIMARY KEY (`id`)," +
		"    UNIQUE KEY `key` (`key`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
)

func NewMySQL(dsn string) Database {
	dsn = ensureDSNParam(dsn, "parseTime", "true")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		logger.Fatalf("failed to open database connection (%s): %v", dsn, err)
	}

	_, err = db.Exec(dbSchema)
	if err != nil {
		logger.Fatalf("failed to init database schema(%s): %v", dbSchema, err)
	}
	if err = ensureSchema(db); err != nil {
		logger.Fatalf("failed to upgrade database schema: %v", err)
	}

	mysqlDB = db
	return &MySQL{}
}

func NewMySQLWithTLS(dsn, tlsName, caPath, certPath, keyPath string, isSkipVerify bool) Database {
	// 1. Load and register TLS configuration
	logger.Infof("MySQL TLS CA: %v", caPath)
	logger.Infof("MySQL TLS client cert: %v", certPath)
	logger.Infof("MySQL TLS client key: %v", keyPath)
	logger.Infof("Server certificate verification skipped: %v", isSkipVerify)
	rootCertPool := x509.NewCertPool()
	pem, err := os.ReadFile(caPath)
	if err != nil {
		logger.Fatalf("failed to read CA cert: %v", err)
	}
	if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
		logger.Fatalf("failed to append CA cert")
	}

	var certs []tls.Certificate
	if certPath != "" && keyPath != "" {
		clientCert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			logger.Fatalf("failed to load client cert and key: %v", err)
		}
		certs = []tls.Certificate{clientCert}
	}

	tlsConfig := &tls.Config{
		RootCAs:            rootCertPool,
		Certificates:       certs,
		InsecureSkipVerify: isSkipVerify,
	}

	if err := mysql.RegisterTLSConfig(tlsName, tlsConfig); err != nil {
		logger.Fatalf("failed to register TLS config: %v", err)
	}

	// 2. Append TLS parameter to DSN if missing
	dsn = ensureDSNParam(dsn, "tls", tlsName)

	// 3. Create and return the Database instance
	return NewMySQL(dsn)
}

func (d *MySQL) CountAll() (int, error) {
	var count int
	err := mysqlDB.QueryRow("SELECT COUNT(1) FROM `devices`").Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (d *MySQL) CountByStatus(status string) (int, error) {
	var count int
	err := mysqlDB.QueryRow("SELECT COUNT(1) FROM `devices` WHERE `status`=?", status).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (d *MySQL) DeviceByKey(key string) (*Device, error) {
	device := &Device{}
	err := mysqlDB.QueryRow("SELECT `key`, `token`, `platform`, `app_id`, `provider_id`, `topic`, `status`, `created_at`, `updated_at`, `last_registered_at` FROM `devices` WHERE `key`=? ", key).
		Scan(&device.DeviceKey, &device.DeviceToken, &device.Platform, &device.AppID, &device.ProviderID, &device.Topic, &device.Status, &device.CreatedAt, &device.UpdatedAt, &device.LastRegisteredAt)
	if err != nil {
		return nil, err
	}

	return device, nil
}

func (d *MySQL) SaveDevice(device *Device) (string, error) {
	device.NormalizeDefaults()
	if device.DeviceKey == "" {
		device.DeviceKey = shortuuid.New()
	}

	now := time.Now().UTC()
	_, err := mysqlDB.Exec(
		"INSERT INTO `devices` (`key`,`token`,`platform`,`app_id`,`provider_id`,`topic`,`status`,`created_at`,`updated_at`,`last_registered_at`) VALUES (?,?,?,?,?,?,?,?,?,?) "+
			"ON DUPLICATE KEY UPDATE `token`=VALUES(`token`), `platform`=VALUES(`platform`), `app_id`=VALUES(`app_id`), `provider_id`=VALUES(`provider_id`), `topic`=VALUES(`topic`), `status`=VALUES(`status`), `updated_at`=VALUES(`updated_at`), `last_registered_at`=VALUES(`last_registered_at`)",
		device.DeviceKey,
		device.DeviceToken,
		device.Platform,
		device.AppID,
		device.ProviderID,
		device.Topic,
		device.Status,
		now,
		now,
		device.LastRegisteredAt,
	)
	if err != nil {
		return "", err
	}

	return device.DeviceKey, nil
}

func (d *MySQL) DeleteDeviceByKey(key string) error {
	_, err := mysqlDB.Exec("DELETE FROM `devices` WHERE `key`=?", key)
	return err
}

func (d *MySQL) Close() error {
	return mysqlDB.Close()
}

func ensureSchema(db *sql.DB) error {
	columns := []struct {
		name string
		stmt string
	}{
		{name: "platform", stmt: "ALTER TABLE `devices` ADD COLUMN `platform` VARCHAR(32) NOT NULL DEFAULT 'ios' AFTER `token`"},
		{name: "app_id", stmt: "ALTER TABLE `devices` ADD COLUMN `app_id` VARCHAR(255) NOT NULL DEFAULT 'me.fin.bark' AFTER `platform`"},
		{name: "provider_id", stmt: "ALTER TABLE `devices` ADD COLUMN `provider_id` VARCHAR(255) NOT NULL DEFAULT 'ios_legacy' AFTER `app_id`"},
		{name: "topic", stmt: "ALTER TABLE `devices` ADD COLUMN `topic` VARCHAR(255) NOT NULL DEFAULT 'me.fin.bark' AFTER `provider_id`"},
		{name: "status", stmt: "ALTER TABLE `devices` ADD COLUMN `status` VARCHAR(32) NOT NULL DEFAULT 'active' AFTER `topic`"},
		{name: "created_at", stmt: "ALTER TABLE `devices` ADD COLUMN `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER `status`"},
		{name: "updated_at", stmt: "ALTER TABLE `devices` ADD COLUMN `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER `created_at`"},
		{name: "last_registered_at", stmt: "ALTER TABLE `devices` ADD COLUMN `last_registered_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER `updated_at`"},
	}
	for _, column := range columns {
		exists, err := columnExists(db, column.name)
		if err != nil {
			return err
		}
		if !exists {
			if _, err := db.Exec(column.stmt); err != nil {
				return fmt.Errorf("add column %s: %w", column.name, err)
			}
		}
	}
	indexes := map[string]string{
		"idx_provider_id": "ALTER TABLE `devices` ADD INDEX `idx_provider_id` (`provider_id`)",
		"idx_status":      "ALTER TABLE `devices` ADD INDEX `idx_status` (`status`)",
	}
	for name, stmt := range indexes {
		exists, err := indexExists(db, name)
		if err != nil {
			return err
		}
		if !exists {
			if _, err := db.Exec(stmt); err != nil {
				return fmt.Errorf("add index %s: %w", name, err)
			}
		}
	}
	return nil
}

func columnExists(db *sql.DB, column string) (bool, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(1) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'devices' AND COLUMN_NAME = ?", column).Scan(&count)
	return count > 0, err
}

func indexExists(db *sql.DB, index string) (bool, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(1) FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'devices' AND INDEX_NAME = ?", index).Scan(&count)
	return count > 0, err
}

func ensureDSNParam(dsn, key, value string) string {
	if strings.Contains(dsn, key+"=") {
		return dsn
	}
	if strings.Contains(dsn, "?") {
		return dsn + "&" + key + "=" + value
	}
	return dsn + "?" + key + "=" + value
}
