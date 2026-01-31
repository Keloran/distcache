package registry

import (
	"sync"
	"time"
)

type Peer struct {
	Address   string
	Name      string
	LastSeen  time.Time
	Healthy   bool
}

type Registry struct {
	mu    sync.RWMutex
	peers map[string]*Peer
}

func New() *Registry {
	return &Registry{
		peers: make(map[string]*Peer),
	}
}

func (r *Registry) Add(address, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.peers[address] = &Peer{
		Address:  address,
		Name:     name,
		LastSeen: time.Now(),
		Healthy:  true,
	}
}

func (r *Registry) Remove(address string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.peers, address)
}

func (r *Registry) Get(address string) (*Peer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	peer, ok := r.peers[address]
	return peer, ok
}

func (r *Registry) GetAll() []*Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()

	peers := make([]*Peer, 0, len(r.peers))
	for _, peer := range r.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (r *Registry) GetHealthy() []*Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()

	peers := make([]*Peer, 0)
	for _, peer := range r.peers {
		if peer.Healthy {
			peers = append(peers, peer)
		}
	}
	return peers
}

func (r *Registry) UpdateLastSeen(address string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if peer, ok := r.peers[address]; ok {
		peer.LastSeen = time.Now()
		peer.Healthy = true
	}
}

func (r *Registry) MarkUnhealthy(address string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if peer, ok := r.peers[address]; ok {
		peer.Healthy = false
	}
}

func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.peers)
}

func (r *Registry) Exists(address string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.peers[address]
	return ok
}