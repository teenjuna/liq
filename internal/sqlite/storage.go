package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/teenjuna/liq/internal"
)

var (
	// ErrClosed is returned by Storage methods when the storage has been closed.
	ErrClosed = errors.New("storage is closed")
)

const (
	memory = ":memory:"
)

// Storage is a persistent batch storage backed by SQLite.
type Storage struct {
	cfg *Config
	db  *sql.DB
}

// New creates a new Storage with the provided configuration functions.
//
// Default configuration:
//   - URI: ":memory:" (in-memory database)
//   - Workers: 1
//   - Batches: 1
//   - Cooldown: 0
//
// Returns an error if the SQLite database cannot be opened or initialized.
func New(configFuncs ...ConfigFunc) (*Storage, error) {
	cfg := &Config{}
	cfg.URI(memory)
	cfg.Workers(1)
	cfg.Batches(1)
	for _, cf := range configFuncs {
		cf(cfg)
	}

	db, err := open(cfg)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	if err := setup(db); err != nil {
		return nil, fmt.Errorf("setup: %w", err)
	}

	storage := Storage{
		cfg: cfg,
		db:  db,
	}

	return &storage, nil
}

// Push inserts a new batch into the storage.
//
// The data is encoded bytes of the batch, and size is the number of items in the batch. Returns a
// unique BatchID that can be used to identify this batch.
//
// Returns [ErrClosed] if the storage has been closed.
func (s *Storage) Push(data []byte, size int) (BatchID, error) {
	id := internal.GenerateID()
	_, err := s.db.Exec(
		`
		insert into batch (
			id,
			data,
			size,
			pushed_at,
			claimed,
			claimed_at,
			claimed_times,
			cooldown_end
		) values (
			:id,
			:data,
			:size,
			:pushed_at,
			:claimed,
			:claimed_at,
			:claimed_times,
			:cooldown_end
		)
		`,
		sql.Named("id", id),
		sql.Named("data", data),
		sql.Named("size", size),
		sql.Named("pushed_at", toTimestamp(time.Now())),
		sql.Named("claimed", 0),
		sql.Named("claimed_at", 0),
		sql.Named("claimed_times", 0),
		sql.Named("cooldown_end", 0),
	)
	if err != nil && err.Error() == "sql: database is closed" {
		return "", ErrClosed
	} else if err != nil {
		return "", err
	}

	return id, nil
}

// Claim atomically claims unclaimed batches for processing.
//
// Batches are selected based on the following criteria:
//   - Not already claimed
//   - Cooldown period has elapsed (cooldown_end <= now)
//   - Ordered by push time (oldest first)
//
// The number of batches returned is limited by [Config.Batches].
//
// Returns an empty slice if no batches are available.
// Returns [ErrClosed] if the storage has been closed.
func (s *Storage) Claim() ([]Batch, error) {
	rows, err := s.db.Query(
		`
		update batch
		set
			claimed = 1,
			claimed_at = :now,
			claimed_times = claimed_times + 1
		where
			id in (
				select id from batch
				where
					claimed = 0 and
					cooldown_end <= :now
				order by
					pushed_at asc
				limit :limit
			)
		returning *
		`,
		sql.Named("now", toTimestamp(time.Now())),
		sql.Named("limit", s.cfg.batches),
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	type rawBatch struct {
		ID           string
		Data         []byte
		Size         int
		PushedAt     int64
		Claimed      int
		ClaimedAt    int64
		ClaimedTimes int
		RetryAfter   int64
	}

	batches := make([]Batch, 0, s.cfg.batches)

	for rows.Next() {
		var b rawBatch
		if err := rows.Scan(
			&b.ID,
			&b.Data,
			&b.Size,
			&b.PushedAt,
			&b.Claimed,
			&b.ClaimedAt,
			&b.ClaimedTimes,
			&b.RetryAfter,
		); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		batches = append(batches, Batch{
			ID:           b.ID,
			Data:         b.Data,
			Size:         b.Size,
			PushedAt:     fromTimestamp(b.PushedAt),
			Claimed:      b.Claimed != 0,
			ClaimedAt:    fromTimestamp(b.ClaimedAt),
			ClaimedTimes: b.ClaimedTimes,
			CooldownEnd:  fromTimestamp(b.RetryAfter),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	return batches, nil
}

// Release releases one or more claimed batches back to the queue.
//
// The batches will be reset to unclaimed state and will have a cooldown applied if
// [Config.Cooldown] was configured. During the cooldown period, the batches will not be eligible
// for re-claiming.
//
// This is typically called when batch processing fails.
func (s *Storage) Release(ids ...BatchID) error {
	var cooldownEnd time.Time
	if s.cfg.cooldown != 0 {
		cooldownEnd = time.Now().Add(s.cfg.cooldown)
	}

	_, err := s.db.Exec(
		`
		update batch
		set 
			claimed = 0,
			cooldown_end = :cooldown_end
		where 
			id in (
				select value from json_each(:ids)
			)
		`,
		sql.Named("ids", jsonIDs(ids)),
		sql.Named("cooldown_end", toTimestamp(cooldownEnd)),
	)
	return err
}

// Delete permanently removes one or more batches from the storage.
//
// This should be called after successful batch processing to clean up
// processed data.
func (s *Storage) Delete(ids ...BatchID) error {
	_, err := s.db.Exec(
		`
		delete from batch
		where 
			id in (
				select value from json_each(:ids)
			)
		`,
		sql.Named("ids", jsonIDs(ids)),
	)
	return err
}

// Stats returns current storage statistics.
//
// Returns the total number of batches, total number of items across all batches,
// and the time when the next cooldown will expire (useful for scheduling).
func (s *Storage) Stats() (*Stats, error) {
	var (
		batches         int
		items           int
		nextCooldownEnd int
	)
	err := s.db.QueryRow(
		`
		select 
			coalesce(count(*), 0) as batches,
			coalesce(sum(size), 0) as items,
			coalesce(min(cooldown_end), 0) as next_cooldown_end
		from
			batch
		`,
	).Scan(
		&batches,
		&items,
		&nextCooldownEnd,
	)
	if err != nil {
		return nil, err
	}

	stats := Stats{
		Batches:         batches,
		Items:           items,
		NextCooldownEnd: fromTimestamp(int64(nextCooldownEnd)),
	}

	return &stats, nil
}

// Close closes the underlying SQLite database.
//
// After closing, all methods on Storage will return [ErrClosed].
func (s *Storage) Close() error {
	return s.db.Close()
}

// Batch represents a stored batch of items in the queue.
type Batch struct {
	// ID is the unique identifier of this batch.
	ID BatchID
	// Data is the encoded batch content.
	Data []byte
	// Size is the number of items in the batch.
	Size int
	// PushedAt is the time when the batch was originally pushed.
	PushedAt time.Time
	// Claimed indicates whether this batch is currently claimed by a worker.
	Claimed bool
	// ClaimedAt is the time when the batch was claimed.
	ClaimedAt time.Time
	// ClaimedTimes is the number of times this batch has been claimed.
	ClaimedTimes int
	// CooldownEnd is the earliest time when this batch can be re-claimed.
	CooldownEnd time.Time
}

type BatchID = string

// Stats represents statistics about the storage.
type Stats struct {
	// Batches is the total number of batches in storage.
	Batches int
	// Items is the total number of items across all batches.
	Items int
	// NextCooldownEnd is the earliest time when any batch becomes available for re-claiming.
	NextCooldownEnd time.Time
}

func open(cfg *Config) (*sql.DB, error) {
	params := url.Values{}
	params.Add("_txlock", "immediate")
	params.Add("_timeout", "5000") // 5s
	params.Add("_foreign_keys", "on")
	if cfg.uri.Opaque == memory {
		cfg.uri.Opaque = internal.GenerateID()
		params.Add("mode", "memory")
		params.Add("cache", "shared")
	} else {
		params.Add("_journal", "wal")
		params.Add("_sync", "normal")
		params.Add("_cache_size", "-20000") // 20mb
	}
	for k, v := range cfg.uri.Query() {
		if len(v) != 0 {
			params.Set(k, v[0])
		}
	}

	cfg.uri.RawQuery = params.Encode()

	db, err := sql.Open("sqlite3", cfg.uri.String())
	if err != nil {
		return nil, err
	}

	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)
	if params.Get("mode") == "memory" {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	} else {
		db.SetMaxOpenConns(cfg.workers)
		db.SetMaxIdleConns(cfg.workers)
	}

	return db, nil
}

func setup(db *sql.DB) error {
	// Create table for batches.
	if _, err := db.Exec(
		`
		create table if not exists batch (
			id            text primary key,
			data          blob not null,
			size          int not null,
			pushed_at     int not null,
			claimed       int not null,
			claimed_at    int not null,
			claimed_times int not null,
			cooldown_end  int not null
		) strict
		`,
	); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// Create the index for the claim logic.
	if _, err := db.Exec(
		`
		create index if not exists idx_batch_ready_to_claim
		on batch (pushed_at, cooldown_end, id)
		where claimed = 0
		`,
	); err != nil {
		return fmt.Errorf("create index: %w", err)
	}

	// Create the index for the stats logic.
	if _, err := db.Exec(
		`
		create index if not exists idx_batch_stats
		on batch (cooldown_end, size, id);
		`,
	); err != nil {
		return fmt.Errorf("create index: %w", err)
	}

	// Just in case the database already existed and previous closing didn't release everything.
	if _, err := db.Exec("update batch set claimed = 0"); err != nil {
		return fmt.Errorf("release batches: %w", err)
	}

	return nil
}

func jsonIDs(ids []BatchID) string {
	jsonIDs, _ := json.Marshal(ids)
	return string(jsonIDs)
}

func toTimestamp(time time.Time) int64 {
	return time.UnixNano()
}

func fromTimestamp(timestamp int64) time.Time {
	return time.Unix(0, timestamp)
}
