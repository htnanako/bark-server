package database

// Database defines all of the db operation
type Database interface {
	CountAll() (int, error)                    // Get db records count
	CountByStatus(status string) (int, error)  // Get db records count by status
	DeviceByKey(key string) (*Device, error)   // Get specified device
	SaveDevice(device *Device) (string, error) // Create or update device
	DeleteDeviceByKey(key string) error        // Delete specified device
	Close() error                              // Close the database
}
