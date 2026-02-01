package cache

import (
	"sync"
	"time"

	ConfigBuilder "github.com/keloran/go-config"
)

type Data struct {
	Timestamp time.Time
	Content   []byte
}

type Cache struct {
	mu      sync.RWMutex
	entries map[string][]*Data
	ttl     time.Duration
}

func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		entries: make(map[string][]*Data),
		ttl:     ttl,
	}
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
func (s *System) CreateEntry(data []byte) {
	s.CreateEntryWithTimestamp(data, time.Now())
}

// CreateEntryWithTimestamp adds a new entry with a specific timestamp (used for replication)
func (s *System) CreateEntryWithTimestamp(data []byte, timestamp time.Time) {
	cacheData := &Data{
		Timestamp: timestamp,
		Content:   data,
	}

	s.Cache.mu.Lock()
	defer s.Cache.mu.Unlock()

	// Clean expired entries for this key before adding new one
	if s.Cache.ttl > 0 && s.Cache.entries[s.Key] != nil {
		s.cleanExpiredEntriesLocked()
	}

	s.Cache.entries[s.Key] = append(s.Cache.entries[s.Key], cacheData)
}

// cleanExpiredEntriesLocked removes expired entries (must be called with lock held)
func (s *System) cleanExpiredEntriesLocked() {
	now := time.Now()
	entries := s.Cache.entries[s.Key]
	if entries == nil {
		return
	}

	// Filter out expired entries
	validEntries := make([]*Data, 0, len(entries))
	for _, entry := range entries {
		if now.Sub(entry.Timestamp) < s.Cache.ttl {
			validEntries = append(validEntries, entry)
		}
	}

	if len(validEntries) == 0 {
		delete(s.Cache.entries, s.Key)
	} else {
		s.Cache.entries[s.Key] = validEntries
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
	validEntries := make([]*Data, 0, len(entries))
	for _, entry := range entries {
		if now.Sub(entry.Timestamp) < filterBefore {
			validEntries = append(validEntries, entry)
		}
	}

	if len(validEntries) == 0 {
		delete(s.Cache.entries, s.Key)
	} else {
		s.Cache.entries[s.Key] = validEntries
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
		validEntries := make([]*Data, 0, len(entries))
		for _, entry := range entries {
			if now.Sub(entry.Timestamp) < c.ttl {
				validEntries = append(validEntries, entry)
			}
		}

		if len(validEntries) == 0 {
			delete(c.entries, key)
		} else {
			c.entries[key] = validEntries
		}
	}
}

// GetTTL returns the cache TTL
func (c *Cache) GetTTL() time.Duration {
	return c.ttl
}
