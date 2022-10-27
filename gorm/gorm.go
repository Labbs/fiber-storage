package gorm

import (
	"database/sql"
	"time"

	"gorm.io/gorm"
)

// Storage interface that is implemented by storage providers
type Storage struct {
	db         *gorm.DB
	gcInterval time.Duration
	done       chan struct{}
}

type Session struct {
	K string `gorm:"primaryKey"`
	V string `gorm:"type:blob"`
	E int64  `gorm:"default:0"`
}

func (Session) TableName() string {
	return cfg.Table
}

var (
	cfg Config
)

// New creates a new storage
func New(config ...Config) *Storage {
	// Set default config
	cfg = configDefault(config...)

	// Drop table if Clear set to true
	if cfg.Reset {
		err := cfg.DB.Migrator().DropTable(&Session{})
		if err != nil {
			panic(err)
		}
	}

	cfg.DB.AutoMigrate(&Session{})

	// Create storage
	store := &Storage{
		gcInterval: cfg.GCInterval,
		db:         cfg.DB,
		done:       make(chan struct{}),
	}

	// Start garbage collector
	go store.gcTicker()

	return store
}

// Get value by key
func (s *Storage) Get(key string) ([]byte, error) {
	if len(key) <= 0 {
		return nil, nil
	}

	var sess Session

	err := s.db.Where("k = ?", key).First(&sess).Error

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	// If the expiration time has already passed, then return nil
	if sess.E != 0 && sess.E <= time.Now().Unix() {
		return nil, nil
	}

	return []byte(sess.V), nil
}

// Set key with value
// Set key with value
func (s *Storage) Set(key string, val []byte, exp time.Duration) error {
	// Ain't Nobody Got Time For That
	if len(key) <= 0 || len(val) <= 0 {
		return nil
	}
	var expSeconds int64
	if exp != 0 {
		expSeconds = time.Now().Add(exp).Unix()
	}
	err := s.db.Create(&Session{K: key, V: string(val), E: expSeconds}).Error
	return err
}

// Delete key by key
func (s *Storage) Delete(key string) error {
	// Ain't Nobody Got Time For That
	if len(key) <= 0 {
		return nil
	}
	err := s.db.Unscoped().Where("k = ?", key).Delete(&Session{}).Error
	return err
}

// Reset all keys
func (s *Storage) Reset() error {
	err := s.db.Unscoped().Delete(&Session{}).Error
	return err
}

// Close the database
func (s *Storage) Close() error {
	s.done <- struct{}{}
	return nil
}

// gcTicker starts the gc ticker
func (s *Storage) gcTicker() {
	ticker := time.NewTicker(s.gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case t := <-ticker.C:
			s.gc(t)
		}
	}
}

// gc deletes all expired entries
func (s *Storage) gc(t time.Time) {
	s.db.Unscoped().Where("e <= ? AND e != 0", t.Unix()).Delete(&Session{})
}
