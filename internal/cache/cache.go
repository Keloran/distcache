package cache

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bugfixes/go-bugfixes/logs"
	ConfigBuilder "github.com/keloran/go-config"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// Default max cache size: 10MB
const DefaultMaxSize int64 = 10 * 1024 * 1024

type Data struct {
	Timestamp time.Time
	Value     *structpb.Value
	size      int64 // cached size of this entry
}

type Cache struct {
	mu          sync.RWMutex
	entries     map[string]map[int64]*Data // key -> timestamp -> data
	ttl         time.Duration
	maxSize     int64 // 0 means unlimited
	currentSize int64
}

// NewCache creates a new cache with the given TTL and default max size (10MB)
func NewCache(ttl time.Duration) *Cache {
	return NewCacheWithMaxSize(ttl, DefaultMaxSize)
}

// NewCacheWithMaxSize creates a new cache with the given TTL and max size
// maxSize of 0 means unlimited
func NewCacheWithMaxSize(ttl time.Duration, maxSize int64) *Cache {
	return &Cache{
		entries: make(map[string]map[int64]*Data),
		ttl:     ttl,
		maxSize: maxSize,
	}
}

// ParseSize parses a human-readable size string (e.g., "10mb", "1g", "32t")
// Returns the size in bytes
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" || s == "0" {
		return 0, nil
	}

	// Match number followed by optional unit
	re := regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([kmgt]?b?)?$`)
	matches := re.FindStringSubmatch(s)
	if matches == nil {
		// Try parsing as plain number (bytes)
		return strconv.ParseInt(s, 10, 64)
	}

	num, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, err
	}

	unit := matches[2]
	multiplier := int64(1)

	switch {
	case strings.HasPrefix(unit, "k"):
		multiplier = 1024
	case strings.HasPrefix(unit, "m"):
		multiplier = 1024 * 1024
	case strings.HasPrefix(unit, "g"):
		multiplier = 1024 * 1024 * 1024
	case strings.HasPrefix(unit, "t"):
		multiplier = 1024 * 1024 * 1024 * 1024
	}

	return int64(num * float64(multiplier)), nil
}

// estimateSize estimates the memory size of a structpb.Value
func estimateSize(v *structpb.Value) int64 {
	if v == nil {
		return 0
	}
	// Use protobuf's Size() for accurate serialized size, add overhead for Go struct
	return int64(proto.Size(v)) + 64 // 64 bytes overhead for Data struct and pointers
}

type System struct {
	Config ConfigBuilder.Config
	Cache  *Cache
	Key    string
}

func New(c *ConfigBuilder.Config, key string, cache *Cache) *System {
	return &System{
		Config: *c,
		Cache:  cache,
		Key:    key,
	}
}

func (s *System) KeyExists() bool {
	s.Cache.mu.RLock()
	defer s.Cache.mu.RUnlock()

	if s.Cache.entries != nil {
		if entries, ok := s.Cache.entries[s.Key]; ok {
			// Check if any non-expired entries exist
			now := time.Now()
			for _, entry := range entries {
				if s.Cache.ttl == 0 || now.Sub(entry.Timestamp) < s.Cache.ttl {
					return true
				}
			}
		}
	}

	return false
}

// CreateEntry adds a new entry and cleans up expired entries for this key
// Returns (true, nil) if stored, (false, nil) if duplicate, or (false, error) on error
func (s *System) CreateEntry(value *structpb.Value) (bool, error) {
	return s.CreateEntryWithTimestamp(value, time.Now())
}

// ErrEntryTooLarge is returned when an entry exceeds the maximum cache size
var ErrEntryTooLarge = fmt.Errorf("entry exceeds maximum cache size")

// CreateEntryWithTimestamp adds a new entry with a specific timestamp (used for replication)
// If an entry with the same key+timestamp exists, it's a no-op (idempotent for replication)
// Returns (true, nil) if a new entry was stored, (false, nil) if it was a duplicate, or (false, error) on error
func (s *System) CreateEntryWithTimestamp(value *structpb.Value, timestamp time.Time) (bool, error) {
	entrySize := estimateSize(value)
	tsNano := timestamp.UnixNano()

	// Reject entries larger than max cache size
	if s.Cache.maxSize > 0 && entrySize > s.Cache.maxSize {
		logs.Warnf("rejecting entry for key %q: size %d exceeds max cache size %d", s.Key, entrySize, s.Cache.maxSize)
		return false, ErrEntryTooLarge
	}

	s.Cache.mu.Lock()
	defer s.Cache.mu.Unlock()

	// Initialize inner map if needed
	if s.Cache.entries[s.Key] == nil {
		s.Cache.entries[s.Key] = make(map[int64]*Data)
	}

	// If this exact timestamp already exists, it's a no-op (idempotent)
	if _, exists := s.Cache.entries[s.Key][tsNano]; exists {
		return false, nil
	}

	// Clean expired entries for this key before adding new one
	if s.Cache.ttl > 0 {
		s.cleanExpiredEntriesLocked()
	}

	// Evict old entries if we would exceed max size
	if s.Cache.maxSize > 0 {
		s.Cache.evictIfNeededLocked(entrySize)
	}

	// Re-initialize inner map if it was deleted by cleanup/eviction
	if s.Cache.entries[s.Key] == nil {
		s.Cache.entries[s.Key] = make(map[int64]*Data)
	}

	cacheData := &Data{
		Timestamp: timestamp,
		Value:     value,
		size:      entrySize,
	}

	s.Cache.entries[s.Key][tsNano] = cacheData
	s.Cache.currentSize += entrySize
	return true, nil
}

// cleanExpiredEntriesLocked removes expired entries (must be called with lock held)
func (s *System) cleanExpiredEntriesLocked() {
	now := time.Now()
	entries := s.Cache.entries[s.Key]
	if entries == nil {
		return
	}

	// Remove expired entries
	for tsNano, entry := range entries {
		if now.Sub(entry.Timestamp) >= s.Cache.ttl {
			s.Cache.currentSize -= entry.size
			delete(entries, tsNano)
		}
	}

	if len(entries) == 0 {
		delete(s.Cache.entries, s.Key)
	}
}

// GetEntries returns all non-expired entries for this key
func (s *System) GetEntries() []*Data {
	s.Cache.mu.RLock()
	defer s.Cache.mu.RUnlock()

	entries := s.Cache.entries[s.Key]
	if entries == nil {
		return nil
	}

	// Filter out expired entries
	now := time.Now()
	result := make([]*Data, 0, len(entries))
	for _, entry := range entries {
		if s.Cache.ttl == 0 || now.Sub(entry.Timestamp) < s.Cache.ttl {
			result = append(result, entry)
		}
	}

	return result
}

func (s *System) RemoveKey() {
	s.Cache.mu.Lock()
	defer s.Cache.mu.Unlock()

	// Subtract size of removed entries
	if entries := s.Cache.entries[s.Key]; entries != nil {
		for _, entry := range entries {
			s.Cache.currentSize -= entry.size
		}
	}
	delete(s.Cache.entries, s.Key)
}

func (s *System) RemoveTimedEntries(filterBefore time.Duration) {
	s.Cache.mu.Lock()
	defer s.Cache.mu.Unlock()

	entries := s.Cache.entries[s.Key]
	if entries == nil {
		return
	}

	now := time.Now()
	for tsNano, entry := range entries {
		if now.Sub(entry.Timestamp) >= filterBefore {
			s.Cache.currentSize -= entry.size
			delete(entries, tsNano)
		}
	}

	if len(entries) == 0 {
		delete(s.Cache.entries, s.Key)
	}
}

// CleanAllExpired removes all expired entries from the entire cache
func (c *Cache) CleanAllExpired() {
	if c.ttl == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, entries := range c.entries {
		for tsNano, entry := range entries {
			if now.Sub(entry.Timestamp) >= c.ttl {
				c.currentSize -= entry.size
				delete(entries, tsNano)
			}
		}

		if len(entries) == 0 {
			delete(c.entries, key)
		}
	}
}

// GetTTL returns the cache TTL
func (c *Cache) GetTTL() time.Duration {
	return c.ttl
}

// GetAllEntries returns all non-expired entries in the cache, keyed by their cache key
func (c *Cache) GetAllEntries() map[string][]*Data {
	return c.getAllEntries(true)
}

// GetAllEntriesForSync returns all entries in the cache regardless of TTL
// Used for syncing to peers - the receiving peer will apply its own TTL
func (c *Cache) GetAllEntriesForSync() map[string][]*Data {
	return c.getAllEntries(false)
}

func (c *Cache) getAllEntries(filterExpired bool) map[string][]*Data {
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now()
	result := make(map[string][]*Data)

	for key, entries := range c.entries {
		validEntries := make([]*Data, 0, len(entries))
		for _, entry := range entries {
			if !filterExpired || c.ttl == 0 || now.Sub(entry.Timestamp) < c.ttl {
				validEntries = append(validEntries, entry)
			}
		}
		if len(validEntries) > 0 {
			result[key] = validEntries
		}
	}

	return result
}

// ImportEntry adds an entry directly (used for sync, skips TTL cleanup)
// If an entry with the same key+timestamp exists, it's a no-op (idempotent)
// Returns error if the entry is larger than the max cache size
func (c *Cache) ImportEntry(key string, value *structpb.Value, timestamp time.Time) error {
	entrySize := estimateSize(value)
	tsNano := timestamp.UnixNano()

	// Reject entries larger than max cache size
	if c.maxSize > 0 && entrySize > c.maxSize {
		logs.Warnf("rejecting import for key %q: size %d exceeds max cache size %d", key, entrySize, c.maxSize)
		return ErrEntryTooLarge
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Initialize inner map if needed
	if c.entries[key] == nil {
		c.entries[key] = make(map[int64]*Data)
	}

	// If this exact timestamp already exists, it's a no-op (idempotent)
	if _, exists := c.entries[key][tsNano]; exists {
		return nil
	}

	// Evict old entries if we would exceed max size
	if c.maxSize > 0 {
		c.evictIfNeededLocked(entrySize)
	}

	data := &Data{
		Timestamp: timestamp,
		Value:     value,
		size:      entrySize,
	}
	c.entries[key][tsNano] = data
	c.currentSize += entrySize
	return nil
}

// evictIfNeededLocked removes the oldest entries until there's room for newEntrySize
// Must be called with lock held
func (c *Cache) evictIfNeededLocked(newEntrySize int64) {
	targetSize := c.maxSize - newEntrySize
	if targetSize < 0 {
		targetSize = 0
	}

	for c.currentSize > targetSize && len(c.entries) > 0 {
		// Find the oldest entry across all keys
		var oldestKey string
		var oldestTsNano int64
		var oldestTime time.Time
		first := true

		for key, entries := range c.entries {
			for tsNano, entry := range entries {
				if first || entry.Timestamp.Before(oldestTime) {
					oldestKey = key
					oldestTsNano = tsNano
					oldestTime = entry.Timestamp
					first = false
				}
			}
		}

		if first {
			// No entries found
			break
		}

		// Remove the oldest entry
		entries := c.entries[oldestKey]
		evicted := entries[oldestTsNano]
		c.currentSize -= evicted.size
		delete(entries, oldestTsNano)

		if len(entries) == 0 {
			delete(c.entries, oldestKey)
		}

		logs.Infof("evicted cache entry for key %q (age: %v) to make room, current size: %d/%d bytes",
			oldestKey, time.Since(oldestTime), c.currentSize, c.maxSize)
	}
}

// GetCurrentSize returns the current cache size in bytes
func (c *Cache) GetCurrentSize() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentSize
}

// GetMaxSize returns the maximum cache size in bytes (0 = unlimited)
func (c *Cache) GetMaxSize() int64 {
	return c.maxSize
}
