package registry

import (
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	r := New()
	if r == nil {
		t.Fatal("New() returned nil")
	}
	if r.peers == nil {
		t.Fatal("peers map is nil")
	}
	if len(r.peers) != 0 {
		t.Errorf("peers map should be empty, got %d entries", len(r.peers))
	}
}

func TestRegistry_Add(t *testing.T) {
	r := New()

	r.Add("192.168.1.1:42069", "node-1")

	peer, ok := r.Get("192.168.1.1:42069")
	if !ok {
		t.Fatal("peer not found after Add")
	}
	if peer.Address != "192.168.1.1:42069" {
		t.Errorf("Address = %q, want %q", peer.Address, "192.168.1.1:42069")
	}
	if peer.Name != "node-1" {
		t.Errorf("Name = %q, want %q", peer.Name, "node-1")
	}
	if !peer.Healthy {
		t.Error("new peer should be healthy")
	}
	if peer.LastSeen.IsZero() {
		t.Error("LastSeen should be set")
	}
}

func TestRegistry_Add_Overwrite(t *testing.T) {
	r := New()

	r.Add("192.168.1.1:42069", "node-1")
	time.Sleep(10 * time.Millisecond)
	r.Add("192.168.1.1:42069", "node-1-updated")

	peer, ok := r.Get("192.168.1.1:42069")
	if !ok {
		t.Fatal("peer not found")
	}
	if peer.Name != "node-1-updated" {
		t.Errorf("Name = %q, want %q", peer.Name, "node-1-updated")
	}
	if r.Count() != 1 {
		t.Errorf("Count() = %d, want 1", r.Count())
	}
}

func TestRegistry_Remove(t *testing.T) {
	r := New()

	r.Add("192.168.1.1:42069", "node-1")
	r.Add("192.168.1.2:42069", "node-2")

	r.Remove("192.168.1.1:42069")

	if _, ok := r.Get("192.168.1.1:42069"); ok {
		t.Error("peer should have been removed")
	}
	if _, ok := r.Get("192.168.1.2:42069"); !ok {
		t.Error("other peer should still exist")
	}
	if r.Count() != 1 {
		t.Errorf("Count() = %d, want 1", r.Count())
	}
}

func TestRegistry_Remove_NonExistent(t *testing.T) {
	r := New()

	// Should not panic when removing non-existent peer
	r.Remove("does-not-exist:42069")

	if r.Count() != 0 {
		t.Errorf("Count() = %d, want 0", r.Count())
	}
}

func TestRegistry_Get(t *testing.T) {
	r := New()

	r.Add("192.168.1.1:42069", "node-1")

	// Test existing peer
	peer, ok := r.Get("192.168.1.1:42069")
	if !ok {
		t.Error("Get should return true for existing peer")
	}
	if peer == nil {
		t.Error("peer should not be nil")
	}

	// Test non-existing peer
	peer, ok = r.Get("does-not-exist:42069")
	if ok {
		t.Error("Get should return false for non-existing peer")
	}
	if peer != nil {
		t.Error("peer should be nil for non-existing address")
	}
}

func TestRegistry_GetAll(t *testing.T) {
	r := New()

	// Empty registry
	peers := r.GetAll()
	if len(peers) != 0 {
		t.Errorf("GetAll() on empty registry returned %d peers, want 0", len(peers))
	}

	// Add peers
	r.Add("192.168.1.1:42069", "node-1")
	r.Add("192.168.1.2:42069", "node-2")
	r.Add("192.168.1.3:42069", "node-3")

	peers = r.GetAll()
	if len(peers) != 3 {
		t.Errorf("GetAll() returned %d peers, want 3", len(peers))
	}

	// Verify all peers are present
	addresses := make(map[string]bool)
	for _, p := range peers {
		addresses[p.Address] = true
	}
	for _, addr := range []string{"192.168.1.1:42069", "192.168.1.2:42069", "192.168.1.3:42069"} {
		if !addresses[addr] {
			t.Errorf("peer %s not found in GetAll() result", addr)
		}
	}
}

func TestRegistry_GetHealthy(t *testing.T) {
	r := New()

	r.Add("192.168.1.1:42069", "node-1")
	r.Add("192.168.1.2:42069", "node-2")
	r.Add("192.168.1.3:42069", "node-3")

	// All should be healthy initially
	healthy := r.GetHealthy()
	if len(healthy) != 3 {
		t.Errorf("GetHealthy() returned %d peers, want 3", len(healthy))
	}

	// Mark one unhealthy
	r.MarkUnhealthy("192.168.1.2:42069")

	healthy = r.GetHealthy()
	if len(healthy) != 2 {
		t.Errorf("GetHealthy() returned %d peers, want 2", len(healthy))
	}

	// Verify the unhealthy peer is not in the list
	for _, p := range healthy {
		if p.Address == "192.168.1.2:42069" {
			t.Error("unhealthy peer should not be in GetHealthy() result")
		}
	}
}

func TestRegistry_GetHealthy_Empty(t *testing.T) {
	r := New()

	healthy := r.GetHealthy()
	if len(healthy) != 0 {
		t.Errorf("GetHealthy() on empty registry returned %d peers, want 0", len(healthy))
	}
}

func TestRegistry_GetHealthy_AllUnhealthy(t *testing.T) {
	r := New()

	r.Add("192.168.1.1:42069", "node-1")
	r.Add("192.168.1.2:42069", "node-2")

	r.MarkUnhealthy("192.168.1.1:42069")
	r.MarkUnhealthy("192.168.1.2:42069")

	healthy := r.GetHealthy()
	if len(healthy) != 0 {
		t.Errorf("GetHealthy() returned %d peers, want 0", len(healthy))
	}
}

func TestRegistry_UpdateLastSeen(t *testing.T) {
	r := New()

	r.Add("192.168.1.1:42069", "node-1")
	peer, _ := r.Get("192.168.1.1:42069")
	initialLastSeen := peer.LastSeen

	// Mark unhealthy first
	r.MarkUnhealthy("192.168.1.1:42069")
	peer, _ = r.Get("192.168.1.1:42069")
	if peer.Healthy {
		t.Error("peer should be unhealthy")
	}

	time.Sleep(10 * time.Millisecond)
	r.UpdateLastSeen("192.168.1.1:42069")

	peer, _ = r.Get("192.168.1.1:42069")
	if !peer.LastSeen.After(initialLastSeen) {
		t.Error("LastSeen should have been updated")
	}
	if !peer.Healthy {
		t.Error("UpdateLastSeen should mark peer as healthy")
	}
}

func TestRegistry_UpdateLastSeen_NonExistent(t *testing.T) {
	r := New()

	// Should not panic when updating non-existent peer
	r.UpdateLastSeen("does-not-exist:42069")
}

func TestRegistry_MarkUnhealthy(t *testing.T) {
	r := New()

	r.Add("192.168.1.1:42069", "node-1")

	peer, _ := r.Get("192.168.1.1:42069")
	if !peer.Healthy {
		t.Error("peer should be healthy initially")
	}

	r.MarkUnhealthy("192.168.1.1:42069")

	peer, _ = r.Get("192.168.1.1:42069")
	if peer.Healthy {
		t.Error("peer should be unhealthy after MarkUnhealthy")
	}
}

func TestRegistry_MarkUnhealthy_NonExistent(t *testing.T) {
	r := New()

	// Should not panic when marking non-existent peer
	r.MarkUnhealthy("does-not-exist:42069")
}

func TestRegistry_Count(t *testing.T) {
	r := New()

	if r.Count() != 0 {
		t.Errorf("Count() = %d, want 0", r.Count())
	}

	r.Add("192.168.1.1:42069", "node-1")
	if r.Count() != 1 {
		t.Errorf("Count() = %d, want 1", r.Count())
	}

	r.Add("192.168.1.2:42069", "node-2")
	if r.Count() != 2 {
		t.Errorf("Count() = %d, want 2", r.Count())
	}

	r.Remove("192.168.1.1:42069")
	if r.Count() != 1 {
		t.Errorf("Count() = %d, want 1", r.Count())
	}
}

func TestRegistry_Exists(t *testing.T) {
	r := New()

	r.Add("192.168.1.1:42069", "node-1")

	if !r.Exists("192.168.1.1:42069") {
		t.Error("Exists should return true for existing peer")
	}
	if r.Exists("does-not-exist:42069") {
		t.Error("Exists should return false for non-existing peer")
	}
}

func TestRegistry_ConcurrentAccess(t *testing.T) {
	r := New()
	var wg sync.WaitGroup
	iterations := 100

	// Concurrent adds
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			addr := "192.168.1." + string(rune('0'+idx%10)) + ":42069"
			r.Add(addr, "node")
		}(i)
	}

	// Concurrent reads
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.GetAll()
			r.GetHealthy()
			r.Count()
		}()
	}

	// Concurrent updates
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			addr := "192.168.1." + string(rune('0'+idx%10)) + ":42069"
			if idx%2 == 0 {
				r.MarkUnhealthy(addr)
			} else {
				r.UpdateLastSeen(addr)
			}
		}(i)
	}

	wg.Wait()

	// Should not panic and registry should be in consistent state
	count := r.Count()
	if count < 0 || count > 10 {
		t.Errorf("unexpected count after concurrent operations: %d", count)
	}
}

func TestRegistry_ConcurrentAddRemove(t *testing.T) {
	r := New()
	var wg sync.WaitGroup

	// Add and remove the same address concurrently
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			r.Add("192.168.1.1:42069", "node-1")
		}()
		go func() {
			defer wg.Done()
			r.Remove("192.168.1.1:42069")
		}()
	}

	wg.Wait()

	// Should not panic - final state can be either present or absent
	_ = r.Exists("192.168.1.1:42069")
}
