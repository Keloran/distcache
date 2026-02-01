package cache

import (
	"sync"
	"testing"
	"time"

	pb "github.com/keloran/distcache/proto/cache"
	ConfigBuilder "github.com/keloran/go-config"
	"google.golang.org/protobuf/types/known/structpb"
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

func stringValue(s string) *pb.CacheValue {
	return &pb.CacheValue{Value: structpb.NewStringValue(s)}
}

func intValue(i int64) *pb.CacheValue {
	return &pb.CacheValue{Value: structpb.NewNumberValue(float64(i))}
}

func floatValue(f float64) *pb.CacheValue {
	return &pb.CacheValue{Value: structpb.NewNumberValue(f)}
}

func boolValue(b bool) *pb.CacheValue {
	return &pb.CacheValue{Value: structpb.NewBoolValue(b)}
}

func bytesValue(b []byte) *pb.CacheValue {
	return &pb.CacheValue{Value: structpb.NewStringValue(string(b))}
}

func structValue(m map[string]interface{}) *pb.CacheValue {
	s, _ := structpb.NewStruct(m)
	return &pb.CacheValue{Value: structpb.NewStructValue(s)}
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

	sys.CreateEntry(stringValue("test-data"))

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

func TestSystem_CreateEntry_String(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	beforeCreate := time.Now()
	sys.CreateEntry(stringValue("hello world"))
	afterCreate := time.Now()

	if !sys.KeyExists() {
		t.Fatal("key should exist after CreateEntry")
	}

	entries := cache.entries["test-key"]
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	entry := entries[0]
	if entry.Value.GetValue().GetStringValue() != "hello world" {
		t.Errorf("StringValue = %v, want %q", entry.Value.GetValue().GetStringValue(), "hello world")
	}
	if entry.Timestamp.Before(beforeCreate) || entry.Timestamp.After(afterCreate) {
		t.Error("Timestamp should be set to current time")
	}
}

func TestSystem_CreateEntry_Int(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	sys.CreateEntry(intValue(12345))

	entries := cache.entries["test-key"]
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	if entries[0].Value.GetValue().GetNumberValue() != 12345 {
		t.Errorf("IntValue = %v, want 12345", entries[0].Value.GetValue().GetNumberValue())
	}
}

func TestSystem_CreateEntry_Float(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	sys.CreateEntry(floatValue(3.14159))

	entries := cache.entries["test-key"]
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	if entries[0].Value.GetValue().GetNumberValue() != 3.14159 {
		t.Errorf("FloatValue = %v, want 3.14159", entries[0].Value.GetValue().GetNumberValue())
	}
}

func TestSystem_CreateEntry_Bool(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	sys.CreateEntry(boolValue(true))

	entries := cache.entries["test-key"]
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	if entries[0].Value.GetValue().GetBoolValue() != true {
		t.Errorf("BoolValue = %v, want true", entries[0].Value.GetValue().GetBoolValue())
	}
}

func TestSystem_CreateEntry_Bytes(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	jsonData := []byte(`{"name":"test","value":123}`)
	sys.CreateEntry(bytesValue(jsonData))

	entries := cache.entries["test-key"]
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	if entries[0].Value.GetValue().GetStringValue() != string(jsonData) {
		t.Errorf("BytesValue = %v, want %v", entries[0].Value.GetValue().GetStringValue(), string(jsonData))
	}
}

func TestSystem_CreateEntry_Multiple(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	sys.CreateEntry(stringValue("first"))
	sys.CreateEntry(stringValue("second"))
	sys.CreateEntry(stringValue("third"))

	entries := cache.entries["test-key"]
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	if entries[0].Value.GetValue().GetStringValue() != "first" {
		t.Errorf("entries[0] = %v, want %q", entries[0].Value.GetValue().GetStringValue(), "first")
	}
	if entries[1].Value.GetValue().GetStringValue() != "second" {
		t.Errorf("entries[1] = %v, want %q", entries[1].Value.GetValue().GetStringValue(), "second")
	}
	if entries[2].Value.GetValue().GetStringValue() != "third" {
		t.Errorf("entries[2] = %v, want %q", entries[2].Value.GetValue().GetStringValue(), "third")
	}
}

func TestSystem_CreateEntryWithTimestamp(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	customTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	sys.CreateEntryWithTimestamp(stringValue("data"), customTime)

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

	sys.CreateEntry(stringValue("data"))
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

	sys1.CreateEntry(stringValue("data1"))
	sys2.CreateEntry(stringValue("data2"))

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

	sys.CreateEntry(stringValue("first"))
	sys.CreateEntry(stringValue("second"))

	entries := sys.GetEntries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	if entries[0].Value.GetValue().GetStringValue() != "first" {
		t.Errorf("entries[0] = %v, want %q", entries[0].Value.GetValue().GetStringValue(), "first")
	}
	if entries[1].Value.GetValue().GetStringValue() != "second" {
		t.Errorf("entries[1] = %v, want %q", entries[1].Value.GetValue().GetStringValue(), "second")
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

	sys.CreateEntry(stringValue("old-data"))

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

	sys.CreateEntry(stringValue("old-data"))

	// Wait for first entry to expire
	time.Sleep(60 * time.Millisecond)

	// Create new entry - should clean expired ones
	sys.CreateEntry(stringValue("new-data"))

	entries := sys.GetEntries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after cleanup, got %d", len(entries))
	}

	if entries[0].Value.GetValue().GetStringValue() != "new-data" {
		t.Errorf("expected new-data, got %v", entries[0].Value.GetValue().GetStringValue())
	}
}

func TestSystem_RemoveTimedEntries(t *testing.T) {
	cfg := newTestConfig()
	cache := newTestCache()
	sys := New(cfg, "test-key", cache)

	// Create entry
	sys.CreateEntry(stringValue("old-data"))

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
	sys.CreateEntry(stringValue("recent-data"))

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

	sys1.CreateEntry(stringValue("data1"))
	sys2.CreateEntry(stringValue("data2"))

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
	sys.CreateEntry(stringValue("data"))

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
			sys.CreateEntry(intValue(int64(idx)))
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
			sys.CreateEntry(intValue(int64(idx)))
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

	sys1.CreateEntry(stringValue("from-sys1"))
	sys2.CreateEntry(stringValue("from-sys2"))
	sys3.CreateEntry(stringValue("from-sys3")) // Should append to same key as sys1

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
		Value:     stringValue("test content"),
	}

	if data.Timestamp.Year() != 2024 {
		t.Errorf("Timestamp year = %d, want 2024", data.Timestamp.Year())
	}
	if data.Value.GetValue().GetStringValue() != "test content" {
		t.Errorf("Value = %v, want %q", data.Value.GetValue().GetStringValue(), "test content")
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
