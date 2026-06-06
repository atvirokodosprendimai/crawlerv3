// Package rwdb gives a single-writer/many-readers gorm handle.
//
// SQLite uses two physical pools sharing the same file: R unlimited, W=1.
// Postgres/MySQL use MVCC, so R and W can share connections; the W pool is
// optionally smaller to mirror the CQRS contract.
package rwdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Driver identifies the SQL dialect.
type Driver string

const (
	DriverSQLite   Driver = "sqlite"
	DriverPostgres Driver = "postgres"
	DriverMySQL    Driver = "mysql"
)

// DB exposes a read pool R and a write pool W.
type DB struct {
	R      *gorm.DB
	W      *gorm.DB
	Driver Driver
}

// Tx wraps a *gorm.DB inside ReadTX/WriteTX callbacks.
type Tx struct{ *gorm.DB }

// Callback is the body of a transaction.
type Callback func(tx *Tx) error

// ReadTX runs fn inside a read-only transaction on the read pool.
func (db *DB) ReadTX(ctx context.Context, fn Callback) error {
	return db.R.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(&Tx{tx})
	}, &sql.TxOptions{ReadOnly: true})
}

// WriteTX runs fn inside a read-write transaction on the write pool.
func (db *DB) WriteTX(ctx context.Context, fn Callback) error {
	return db.W.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(&Tx{tx})
	})
}

// Config controls New behavior.
type Config struct {
	Driver  Driver
	DSN     string // sqlite: file path or :memory:; pg/mysql: standard DSN
	ReadDSN string // optional override for pg/mysql read replicas
	Debug   bool
}

// New opens both read and write handles.
func New(cfg Config) (*DB, error) {
	if cfg.DSN == "" {
		return nil, errors.New("rwdb: DSN required")
	}
	lvl := logger.Silent
	if cfg.Debug || os.Getenv("DB_LOG_LEVEL") == "debug" {
		lvl = logger.Info
	}
	lg := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             time.Second,
			LogLevel:                  lvl,
			IgnoreRecordNotFoundError: true,
			ParameterizedQueries:      true,
			Colorful:                  true,
		},
	)
	switch cfg.Driver {
	case DriverSQLite, "":
		return newSQLite(cfg, lg)
	case DriverPostgres:
		return newPostgres(cfg, lg)
	case DriverMySQL:
		return newMySQL(cfg, lg)
	}
	return nil, fmt.Errorf("rwdb: unknown driver %q", cfg.Driver)
}

func newSQLite(cfg Config, lg logger.Interface) (*DB, error) {
	open := func(maxConns int) (*gorm.DB, error) {
		gdb, err := gorm.Open(sqlite.Open(cfg.DSN), &gorm.Config{PrepareStmt: true, Logger: lg})
		if err != nil {
			return nil, fmt.Errorf("rwdb sqlite: %w", err)
		}
		s, err := gdb.DB()
		if err != nil {
			return nil, err
		}
		s.SetMaxOpenConns(maxConns)
		s.SetConnMaxLifetime(-1)
		for _, p := range []string{
			"PRAGMA journal_mode=WAL;",
			"PRAGMA synchronous=NORMAL;",
			"PRAGMA foreign_keys=ON;",
			"PRAGMA busy_timeout=5000;",
		} {
			if err := gdb.Exec(p).Error; err != nil {
				return nil, fmt.Errorf("rwdb sqlite pragma %q: %w", p, err)
			}
		}
		return gdb, nil
	}
	r, err := open(runtime.NumCPU())
	if err != nil {
		return nil, err
	}
	w, err := open(1)
	if err != nil {
		return nil, err
	}
	return &DB{R: r, W: w, Driver: DriverSQLite}, nil
}

func newPostgres(cfg Config, lg logger.Interface) (*DB, error) {
	rDSN := cfg.ReadDSN
	if rDSN == "" {
		rDSN = cfg.DSN
	}
	open := func(dsn string, maxOpen int) (*gorm.DB, error) {
		gdb, err := gorm.Open(postgres.Open(dsn), &gorm.Config{PrepareStmt: true, Logger: lg})
		if err != nil {
			return nil, fmt.Errorf("rwdb postgres: %w", err)
		}
		s, err := gdb.DB()
		if err != nil {
			return nil, err
		}
		s.SetMaxOpenConns(maxOpen)
		s.SetConnMaxLifetime(time.Hour)
		return gdb, nil
	}
	r, err := open(rDSN, runtime.NumCPU()*4)
	if err != nil {
		return nil, err
	}
	w, err := open(cfg.DSN, runtime.NumCPU())
	if err != nil {
		return nil, err
	}
	return &DB{R: r, W: w, Driver: DriverPostgres}, nil
}

func newMySQL(cfg Config, lg logger.Interface) (*DB, error) {
	rDSN := cfg.ReadDSN
	if rDSN == "" {
		rDSN = cfg.DSN
	}
	open := func(dsn string, maxOpen int) (*gorm.DB, error) {
		gdb, err := gorm.Open(mysql.Open(dsn), &gorm.Config{PrepareStmt: true, Logger: lg})
		if err != nil {
			return nil, fmt.Errorf("rwdb mysql: %w", err)
		}
		s, err := gdb.DB()
		if err != nil {
			return nil, err
		}
		s.SetMaxOpenConns(maxOpen)
		s.SetConnMaxLifetime(time.Hour)
		return gdb, nil
	}
	r, err := open(rDSN, runtime.NumCPU()*4)
	if err != nil {
		return nil, err
	}
	w, err := open(cfg.DSN, runtime.NumCPU())
	if err != nil {
		return nil, err
	}
	return &DB{R: r, W: w, Driver: DriverMySQL}, nil
}

// Close releases both pools.
func (db *DB) Close() error {
	var errs []error
	for _, h := range []*gorm.DB{db.R, db.W} {
		if h == nil {
			continue
		}
		s, err := h.DB()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := s.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
