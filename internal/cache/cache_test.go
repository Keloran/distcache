package cache

import (
	"bytes"
	"sync"
	"testing"
	"time"

	ConfigBuilder "github.com/keloran/go-config"
)

func newTestCache() *Cache {
	return NewCache(0) // No TTL for basic tests
}

func newTestCacheWithTTL(ttl time.Duration) *Cache {
	return NewCache(ttl)
}

func newTestConfig() *ConfigBuilder.Config {
	return &ConfigBuilder.Config{}
}

func TestNew(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()

	sys := New(cfg, "test-key", cache)

	if sys == nil {
		t.Fatal("New() returned nil")
	}
	if sys.Key != "test-key" {
		t.Errorf("Key = %q, want %q", sys.Key, "test-key")
	}
	if sys.Cache != cache {
		t.Error("Cache not set correctly")
	}
}

func TestNewCache(t *testing.T) {
	cache := NewCache(10 * time.Second)

	if cache == nil {
		t.Fatal("NewCache() returned nil")
	}
	if cache.ttl != 10*time.Second {
		t.Errorf("TTL = %v, want %v", cache.ttl, 10*time.Second)
	}
	if cache.entries == nil {
		t.Error("entries should be initialized")
	}
}

func TestSystem_KeyExists_NotExists(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "missing-key", cache)

	if sys.KeyExists() {
		t.Error("KeyExists() should return false for non-existent key")
	}
}

func TestSystem_KeyExists_Exists(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "my-key", cache)

	sys.CreateEntry([]byte("test-data"))

	if !sys.KeyExists() {
		t.Error("KeyExists() should return true after CreateEntry")
	}
}

func TestSystem_KeyExists_NilEntries(t *testing.T) {
	cfg := newTestConfig()
	cache := &Cache{
		entries: nil, // Explicitly nil
	}
	sys := New(cfg, "test-key", cache)

	if sys.KeyExists() {
		t.Error("KeyExists() should return false when entries is nil")
	}
}

func TestSystem_CreateEntry(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	beforeCreate := time.Now()
	sys.CreateEntry([]byte("hello world"))
	afterCreate := time.Now()

	if !sys.KeyExists() {
		t.Fatal("key should exist after CreateEntry")
	}

	entries := cache.entries["test-key"]
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	entry := entries[0]
	if !bytes.Equal(entry.Content, []byte("hello world")) {
		t.Errorf("Content = %v, want %q", entry.Content, "hello world")
	}
	if entry.Timestamp.Before(beforeCreate) || entry.Timestamp.After(afterCreate) {
		t.Error("Timestamp should be set to current time")
	}
}

func TestSystem_CreateEntry_Multiple(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	sys.CreateEntry([]byte("first"))
	sys.CreateEntry([]byte("second"))
	sys.CreateEntry([]byte("third"))

	entries := cache.entries["test-key"]
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	if !bytes.Equal(entries[0].Content, []byte("first")) {
		t.Errorf("entries[0].Content = %v, want %q", entries[0].Content, "first")
	}
	if !bytes.Equal(entries[1].Content, []byte("second")) {
		t.Errorf("entries[1].Content = %v, want %q", entries[1].Content, "second")
	}
	if !bytes.Equal(entries[2].Content, []byte("third")) {
		t.Errorf("entries[2].Content = %v, want %q", entries[2].Content, "third")
	}
}

func TestSystem_CreateEntryWithTimestamp(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	customTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	sys.CreateEntryWithTimestamp([]byte("data"), customTime)

	entries := cache.entries["test-key"]
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	if !entries[0].Timestamp.Equal(customTime) {
		t.Errorf("Timestamp = %v, want %v", entries[0].Timestamp, customTime)
	}
}

func TestSystem_RemoveKey(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	sys.CreateEntry([]byte("data"))
	if !sys.KeyExists() {
		t.Fatal("key should exist before remove")
	}

	sys.RemoveKey()

	if sys.KeyExists() {
		t.Error("key should not exist after RemoveKey")
	}
}

func TestSystem_RemoveKey_NonExistent(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "missing-key", cache)

	// Should not panic when removing non-existent key
	sys.RemoveKey()

	if sys.KeyExists() {
		t.Error("key should not exist")
	}
}

func TestSystem_RemoveKey_PreservesOtherKeys(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()

	sys1 := New(cfg, "key1", cache)
	sys2 := New(cfg, "key2", cache)

	sys1.CreateEntry([]byte("data1"))
	sys2.CreateEntry([]byte("data2"))

	sys1.RemoveKey()

	if sys1.KeyExists() {
		t.Error("key1 should not exist")
	}
	if !sys2.KeyExists() {
		t.Error("key2 should still exist")
	}
}

func TestSystem_GetEntries(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	sys.CreateEntry([]byte("first"))
	sys.CreateEntry([]byte("second"))

	entries := sys.GetEntries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	if !bytes.Equal(entries[0].Content, []byte("first")) {
		t.Errorf("entries[0].Content = %v, want %q", entries[0].Content, "first")
	}
	if !bytes.Equal(entries[1].Content, []byte("second")) {
		t.Errorf("entries[1].Content = %v, want %q", entries[1].Content, "second")
	}
}

func TestSystem_GetEntries_Empty(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	entries := sys.GetEntries()
	if entries != nil {
		t.Errorf("expected nil entries for non-existent key, got %v", entries)
	}
}

func TestSystem_TTL_ExpiresEntries(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCacheWithTTL(50 * time.Millisecond)
	sys := New(cfg, "test-key", cache)

	sys.CreateEntry([]byte("old-data"))

	// Entry should exist
	if !sys.KeyExists() {
		t.Error("key should exist immediately after creation")
	}

	// Wait for TTL to expire
	time.Sleep(60 * time.Millisecond)

	// Entry should be expired now
	if sys.KeyExists() {
		t.Error("key should not exist after TTL expires")
	}

	entries := sys.GetEntries()
	if len(entries) != 0 {
		t.Errorf("expected 0 entries after TTL, got %d", len(entries))
	}
}

func TestSystem_TTL_CleansOnCreate(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCacheWithTTL(50 * time.Millisecond)
	sys := New(cfg, "test-key", cache)

	sys.CreateEntry([]byte("old-data"))

	// Wait for first entry to expire
	time.Sleep(60 * time.Millisecond)

	// Create new entry - should clean expired ones
	sys.CreateEntry([]byte("new-data"))

	entries := sys.GetEntries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after cleanup, got %d", len(entries))
	}

	if !bytes.Equal(entries[0].Content, []byte("new-data")) {
		t.Errorf("expected new-data, got %v", entries[0].Content)
	}
}

func TestSystem_RemoveTimedEntries(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	// Create entry
	sys.CreateEntry([]byte("old-data"))

	// Wait a bit
	time.Sleep(20 * time.Millisecond)

	// Remove entries older than 10ms
	sys.RemoveTimedEntries(10 * time.Millisecond)

	// Entry should be removed
	if sys.KeyExists() {
		t.Error("key should not exist after RemoveTimedEntries")
	}
}

func TestSystem_RemoveTimedEntries_KeepsRecent(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	// Create entry
	sys.CreateEntry([]byte("recent-data"))

	// Remove entries older than 1 hour (should keep our entry)
	sys.RemoveTimedEntries(time.Hour)

	// Entry should still exist
	if !sys.KeyExists() {
		t.Error("key should still exist")
	}
}

func TestSystem_RemoveTimedEntries_NilEntries(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "missing-key", cache)

	// Should not panic when key doesn't exist
	sys.RemoveTimedEntries(time.Hour)
}

func TestCache_CleanAllExpired(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCacheWithTTL(50 * time.Millisecond)

	sys1 := New(cfg, "key1", cache)
	sys2 := New(cfg, "key2", cache)

	sys1.CreateEntry([]byte("data1"))
	sys2.CreateEntry([]byte("data2"))

	// Wait for entries to expire
	time.Sleep(60 * time.Millisecond)

	// Clean all expired
	cache.CleanAllExpired()

	if sys1.KeyExists() {
		t.Error("key1 should be cleaned")
	}
	if sys2.KeyExists() {
		t.Error("key2 should be cleaned")
	}
}

func TestCache_CleanAllExpired_NoTTL(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache() // No TTL

	sys := New(cfg, "test-key", cache)
	sys.CreateEntry([]byte("data"))

	// Should not remove anything when TTL is 0
	cache.CleanAllExpired()

	if !sys.KeyExists() {
		t.Error("key should still exist when TTL is 0")
	}
}

func TestCache_GetTTL(t *testing.T) {
	cache := NewCache(30 * time.Second)

	if cache.GetTTL() != 30*time.Second {
		t.Errorf("GetTTL() = %v, want %v", cache.GetTTL(), 30*time.Second)
	}
}

func TestSystem_ConcurrentAccess(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "concurrent-key", cache)

	var wg sync.WaitGroup
	iterations := 100

	// Concurrent creates
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sys.CreateEntry([]byte(itoa(idx)))
		}(i)
	}

	// Concurrent reads
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sys.KeyExists()
		}()
	}

	wg.Wait()

	// All entries should be present
	entries := cache.entries["concurrent-key"]
	if len(entries) != iterations {
		t.Errorf("expected %d entries, got %d", iterations, len(entries))
	}
}

func TestSystem_ConcurrentCreateRemove(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()

	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(idx int) {
			defer wg.Done()
			sys := New(cfg, "key-"+itoa(idx%5), cache)
			sys.CreateEntry([]byte(itoa(idx)))
		}(i)
		go func(idx int) {
			defer wg.Done()
			sys := New(cfg, "key-"+itoa(idx%5), cache)
			sys.RemoveKey()
		}(i)
	}

	wg.Wait()

	// Should not panic - final state is indeterminate but consistent
}

func TestSystem_MultipleSystemsSameCache(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()

	sys1 := New(cfg, "key1", cache)
	sys2 := New(cfg, "key2", cache)
	sys3 := New(cfg, "key1", cache) // Same key as sys1

	sys1.CreateEntry([]byte("from-sys1"))
	sys2.CreateEntry([]byte("from-sys2"))
	sys3.CreateEntry([]byte("from-sys3")) // Should append to same key as sys1

	// key1 should have 2 entries
	if len(cache.entries["key1"]) != 2 {
		t.Errorf("key1 should have 2 entries, got %d", len(cache.entries["key1"]))
	}

	// key2 should have 1 entry
	if len(cache.entries["key2"]) != 1 {
		t.Errorf("key2 should have 1 entry, got %d", len(cache.entries["key2"]))
	}

	// sys1 and sys3 should see the same data
	if !sys1.KeyExists() || !sys3.KeyExists() {
		t.Error("sys1 and sys3 should both see key1 exists")
	}
}

func TestData_Fields(t *testing.T) {
	data := &Data{
		Timestamp: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		Content:   []byte("test content"),
	}

	if data.Timestamp.Year() != 2024 {
		t.Errorf("Timestamp year = %d, want 2024", data.Timestamp.Year())
	}
	if !bytes.Equal(data.Content, []byte("test content")) {
		t.Errorf("Content = %v, want %q", data.Content, "test content")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	result := ""
	for i > 0 {
		result = string(rune('0'+i%10)) + result
		i /= 10
	}
	return result
}
