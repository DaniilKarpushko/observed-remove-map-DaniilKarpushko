package node

import (
	"context"
	"sync"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode
	state        MapState
	localCounter uint64
	mu           sync.RWMutex
	nodeIDs      []string
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	n := &CRDTMapNode{
		BaseNode:     hive.NewBaseNode(id),
		state:        make(MapState),
		localCounter: 0,
		nodeIDs:      allNodeIDs,
	}
	n.SetNodeRef(n)
	return n
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {
	if err := n.BaseNode.Start(ctx); err != nil {
		return err
	}

	// Start background anti-entropy broadcast
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Broadcast state to all peers
				n.mu.RLock()
				stateCopy := make(MapState)
				for k, v := range n.state {
					stateCopy[k] = v
				}
				n.mu.RUnlock()

				// Send state to all peers
				for _, peerID := range n.nodeIDs {
					if peerID != n.ID() {
						_ = n.Send(peerID, stateCopy)
					}
				}
			}
		}
	}()

	return nil
}

// nextVersion returns a new version with incremented counter.
func (n *CRDTMapNode) nextVersion() Version {
	n.localCounter++
	return Version{
		Counter: n.localCounter,
		NodeID:  n.ID(),
	}
}

// isGreater checks if version v1 is greater than v2 using LWW ordering.
// Ordering is lexicographic: (Counter, NodeID).
func isGreater(v1, v2 Version) bool {
	if v1.Counter != v2.Counter {
		return v1.Counter > v2.Counter
	}
	return v1.NodeID > v2.NodeID
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, v string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state[k] = StateEntry{
		Value:     v,
		Tombstone: false,
		Version:   n.nextVersion(),
	}
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	entry, exists := n.state[k]
	if !exists || entry.Tombstone {
		return "", false
	}
	return entry.Value, true
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	newVersion := n.nextVersion()
	n.state[k] = StateEntry{
		Value:     "",
		Tombstone: true,
		Version:   newVersion,
	}
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for k, remoteEntry := range remote {
		localEntry, exists := n.state[k]

		if !exists {
			// No local entry, take remote
			n.state[k] = remoteEntry
		} else {
			// Compare versions and keep the greater one
			if isGreater(remoteEntry.Version, localEntry.Version) {
				n.state[k] = remoteEntry
			}
		}
	}
}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	n.mu.RLock()
	defer n.mu.RUnlock()

	stateCopy := make(MapState)
	for k, v := range n.state {
		stateCopy[k] = v
	}
	return stateCopy
}

// ToMap returns a value-only map view without tombstones.
func (n *CRDTMapNode) ToMap() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	result := make(map[string]string)
	for k, v := range n.state {
		if !v.Tombstone {
			result[k] = v.Value
		}
	}
	return result
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	if msg.Payload == nil {
		return nil
	}

	remoteState, ok := msg.Payload.(MapState)
	if !ok {
		return nil
	}

	n.Merge(remoteState)
	return nil
}
