package cache

import (
	"sync"
	"time"

	ConfigBuilder "github.com/keloran/go-config"
)

type Data struct {
	Timestamp time.Time
	Content   interface{}
}

type Cache struct {
	mu      sync.RWMutex
	entries map[string][]*Data
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
		if _, ok := s.Cache.entries[s.Key]; ok {
			return true
		}
	}

	return false
}

func (s *System) CreateEntry(data interface{}) {
	cacheData := &Data{
		Timestamp: time.Now(),
		Content:   data,
	}

	s.Cache.mu.Lock()
	defer s.Cache.mu.Unlock()
	s.Cache.entries[s.Key] = append(s.Cache.entries[s.Key], cacheData)
}

func (s *System) RemoveKey() {
	s.Cache.mu.Lock()
	defer s.Cache.mu.Unlock()
	delete(s.Cache.entries, s.Key)
}

func (s *System) RemoveTimedEntries(filterBefore time.Duration) {
	s.Cache.mu.Lock()
	defer s.Cache.mu.Unlock()
	if s.Cache.entries[s.Key] != nil {
		for _, entry := range s.Cache.entries[s.Key] {
			if entry.Timestamp.Add(filterBefore).After(time.Now()) {
				delete(s.Cache.entries, s.Key)
			}
		}
	}
}
