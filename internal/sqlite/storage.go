package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var (
	ErrClosed = errors.New("storage is closed")
)

type Storage struct {
	cfg *Config
	db  *sql.DB
}

func New(configFuncs ...ConfigFunc) (*Storage, error) {
	cfg := &Config{}
	cfg.File(":memory:")
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

func (s *Storage) Push(data []byte, size int) (BatchID, error) {
	id := generateID()
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

func (s *Storage) Close() error {
	return s.db.Close()
}

type Batch struct {
	ID           BatchID
	Data         []byte
	Size         int
	PushedAt     time.Time
	Claimed      bool
	ClaimedAt    time.Time
	ClaimedTimes int
	CooldownEnd  time.Time
}

type BatchID = string

type Stats struct {
	Batches         int
	Items           int
	NextCooldownEnd time.Time
}

func open(cfg *Config) (*sql.DB, error) {
	params := url.Values{}
	params.Add("_txlock", "immediate")
	params.Add("_timeout", "5000") // 5s
	params.Add("_foreign_keys", "on")
	if cfg.file == ":memory:" {
		cfg.file = generateID()
		params.Add("mode", "memory")
		params.Add("cache", "shared")
	} else {
		params.Add("_journal", "wal")
		params.Add("_sync", "normal")
		params.Add("_cache_size", "-20000") // 20mb
	}

	uri := "file:" + cfg.file + "?" + params.Encode()

	db, err := sql.Open("sqlite3", uri)
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

func generateID() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const n = 10
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rand.IntN(len(charset))]
	}
	return string(b)
}
