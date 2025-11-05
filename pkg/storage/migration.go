package storage

import (
	"errors"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

// SaveMigrationStatus saves the migration status
func (s *Storage) SaveMigrationStatus(status *SystemMigrationStatus) error {
	data, err := msgpack.Marshal(status)
	if err != nil {
		return fmt.Errorf("failed to marshal migration status: %w", err)
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(metaBucket))
		if bucket == nil {
			return fmt.Errorf("meta bucket not found")
		}
		return bucket.Put([]byte(migrationKey), data)
	})
}

// GetMigrationStatus retrieves the migration status
func (s *Storage) GetMigrationStatus() (*SystemMigrationStatus, error) {
	var status SystemMigrationStatus

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(metaBucket))
		if bucket == nil {
			return fmt.Errorf("meta bucket not found")
		}

		data := bucket.Get([]byte(migrationKey))
		if data == nil {
			return errors.New("migration status not found")
		}

		return msgpack.Unmarshal(data, &status)
	})

	if err != nil {
		// Return default status if not found
		return &SystemMigrationStatus{
			Running:   false,
			Total:     0,
			Completed: 0,
			Errors:    0,
		}, nil
	}

	return &status, nil
}

// DeleteMigrationStatus removes migration status
func (s *Storage) DeleteMigrationStatus() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(metaBucket))
		if bucket == nil {
			return fmt.Errorf("meta bucket not found")
		}
		return bucket.Delete([]byte(migrationKey))
	})
}
