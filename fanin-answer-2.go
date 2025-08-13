package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ----------------------------------------------------------------------------
// GOAL OF THIS VERSION
// ----------------------------------------------------------------------------
// 1) No Commands/CommandHandlers. All effects happen inside EventHandlers.
// 2) After parsing, we persist a canonical OrderEvent that can be replayed later
//    to rebuild order state (and optionally suppress side-effects during replay).
// ----------------------------------------------------------------------------

// --- Kafka-like message (minimal) --------------------------------------------

type KafkaMessage struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

func msgID(m KafkaMessage) string { return fmt.Sprintf("%s-%d-%d", m.Topic, m.Partition, m.Offset) }

// --- Canonical persisted event ------------------------------------------------

// OrderEvent is the single, unified structure we save to the DB immediately
// after parsing. It is append-only and suitable for replay.

type OrderEvent struct {
	EventID       string          `json:"event_id"`         // typically topic-partition-offset
	OrderID       string          `json:"order_id"`
	Type          string          `json:"type"`             // e.g., OrderStatusChanged, ShipmentCompanyMetadataReceived
	At            time.Time       `json:"at"`
	Seq           int             `json:"seq"`              // per-order sequence number assigned on append
	SchemaVersion int             `json:"schema_version"`   // evolve payloads safely
	SourceTopic   string          `json:"source_topic"`
	SourcePart    int             `json:"source_partition"`
	SourceOffset  int64           `json:"source_offset"`
	Payload       json.RawMessage `json:"payload"`          // canonical, minimal payload for this Type
}

// ExecutionContext lets us disable side-effects during replay.

type ExecutionContext struct {
	AllowSideEffects bool
}

// --- Event store (append-only, ordered per order) -----------------------------

type EventStore interface {
	Append(ctx context.Context, evt OrderEvent) error
	ListByOrder(ctx context.Context, orderID string) ([]OrderEvent, error)
}

type InMemoryEventStore struct {
	mu       sync.Mutex
	byOrder  map[string][]OrderEvent
	seen     map[string]struct{} // EventID dedupe
}

func NewInMemoryEventStore() *InMemoryEventStore {
	return &InMemoryEventStore{byOrder: map[string][]OrderEvent{}, seen: map[string]struct{}{}}
}

func (s *InMemoryEventStore) Append(ctx context.Context, evt OrderEvent) error {
	s.mu.Lock(); defer s.mu.Unlock()
	if _, dup := s.seen[evt.EventID]; dup {
		log.Printf("[EventStore] duplicate %s ignored", evt.EventID)
		return nil
	}
	seq := len(s.byOrder[evt.OrderID]) + 1
	evt.Seq = seq
	s.byOrder[evt.OrderID] = append(s.byOrder[evt.OrderID], evt)
	s.seen[evt.EventID] = struct{}{}
	log.Printf("[EventStore] appended #%d %s for order %s", evt.Seq, evt.Type, evt.OrderID)
	return nil
}

func (s *InMemoryEventStore) ListByOrder(ctx context.Context, orderID string) ([]OrderEvent, error) {
	s.mu.Lock(); defer s.mu.Unlock()
	evts := s.byOrder[orderID]
	// return a copy to protect internal slice
	out := make([]OrderEvent, len(evts))
	copy(out, evts)
	return out, nil
}

// --- Order repository (current state projection) ------------------------------

type Order struct {
	ID       string                               `json:"id"`
	Status   string                               `json:"status"`
	ShipMeta map[string]map[string]any            `json:"ship_meta"` // carrier -> meta
	History  []OrderEvent                         `json:"-"`         // optional: for debug
}

type OrderRepository interface {
	UpsertStatus(ctx context.Context, orderID, newStatus string) error
	UpsertShipmentMeta(ctx context.Context, orderID, carrier string, meta map[string]any) error
	Replace(ctx context.Context, o Order) error
	Get(ctx context.Context, orderID string) (Order, bool)
	Reset(ctx context.Context) // for demo
}

type InMemoryOrderRepo struct { mu sync.Mutex; store map[string]Order }

func NewInMemoryOrderRepo() *InMemoryOrderRepo { return &InMemoryOrderRepo{store: map[string]Order{}} }

func (r *InMemoryOrderRepo) UpsertStatus(ctx context.Context, orderID, newStatus string) error {
	r.mu.Lock(); defer r.mu.Unlock()
	o := r.store[orderID]
	o.ID = orderID
	o.Status = newStatus
	r.store[orderID] = o
	log.Printf("[OrderRepo] order %s status -> %s", orderID, newStatus)
	return nil
}

func (r *InMemoryOrderRepo) UpsertShipmentMeta(ctx context.Context, orderID, carrier string, meta map[string]any) error {
	r.mu.Lock(); defer r.mu.Unlock()
	o := r.store[orderID]
	if o.ID == "" { o.ID = orderID }
	if o.ShipMeta == nil { o.ShipMeta = map[string]map[string]any{} }
	copied := map[string]any{}
	for k, v := range meta { copied[k] = v }
	o.ShipMeta[carrier] = copied
	r.store[orderID] = o
	log.Printf("[OrderRepo] order %s carrier %s meta upserted: %v", orderID, carrier, meta)
	return nil
}

func (r *InMemoryOrderRepo) Replace(ctx context.Context, o Order) error { r.mu.Lock(); defer r.mu.Unlock(); r.store[o.ID] = o; return nil }
func (r *InMemoryOrderRepo) Get(ctx context.Context, orderID string) (Order, bool) { r.mu.Lock(); defer r.mu.Unlock(); o, ok := r.store[orderID]; return o, ok }
func (r *InMemoryOrderRepo) Reset(ctx context.Context)                    { r.mu.Lock(); defer r.mu.Unlock(); r.store = map[string]Order{} }

// --- Parser per topic -> returns canonical OrderEvent --------------------------

type Parser interface {
	Parse(ctx context.Context, msg KafkaMessage) (OrderEvent, error)
}

type ParserRegistry map[string]Parser // topic -> parser

// StatusParser normalizes to OrderEvent{Type: "OrderStatusChanged", Payload: {"new_status":...}}

type StatusParser struct{}

func (StatusParser) Parse(ctx context.Context, msg KafkaMessage) (OrderEvent, error) {
	var p struct {
		OrderID   string `json:"order_id"`
		NewStatus string `json:"new_status"`
	}
	if err := json.Unmarshal(msg.Value, &p); err != nil { return OrderEvent{}, fmt.Errorf("status parse: %w", err) }
	if p.OrderID == "" || p.NewStatus == "" { return OrderEvent{}, errors.New("status parse: missing fields") }
	payload := mustJSON(map[string]any{"new_status": p.NewStatus})
	return OrderEvent{
		EventID:      msgID(msg),
		OrderID:      p.OrderID,
		Type:         "OrderStatusChanged",
		At:           msg.Timestamp,
		SchemaVersion: 1,
		SourceTopic:  msg.Topic,
		SourcePart:   msg.Partition,
		SourceOffset: msg.Offset,
		Payload:      payload,
	}, nil
}

// ShipmentMetaParser normalizes to OrderEvent{Type: "ShipmentCompanyMetadataReceived", Payload: {carrier, meta}}

type ShipmentMetaParser struct{}

func (ShipmentMetaParser) Parse(ctx context.Context, msg KafkaMessage) (OrderEvent, error) {
	var p struct {
		OrderID string         `json:"order_id"`
		Carrier string         `json:"carrier"`
		Meta    map[string]any `json:"meta"`
	}
	if err := json.Unmarshal(msg.Value, &p); err != nil { return OrderEvent{}, fmt.Errorf("shipment meta parse: %w", err) }
	if p.OrderID == "" || p.Carrier == "" { return OrderEvent{}, errors.New("shipment meta parse: missing fields") }
	payload := mustJSON(map[string]any{"carrier": p.Carrier, "meta": p.Meta})
	return OrderEvent{
		EventID:      msgID(msg),
		OrderID:      p.OrderID,
		Type:         "ShipmentCompanyMetadataReceived",
		At:           msg.Timestamp,
		SchemaVersion: 1,
		SourceTopic:  msg.Topic,
		SourcePart:   msg.Partition,
		SourceOffset: msg.Offset,
		Payload:      payload,
	}, nil
}

// --- Unified EventHandler (does projections + side-effects) --------------------

type EventHandler interface { Handle(ctx context.Context, evt OrderEvent, exec ExecutionContext) error }

type UnifiedHandler struct {
	repo   OrderRepository
	httpC  *http.Client
}

func (h UnifiedHandler) Handle(ctx context.Context, evt OrderEvent, exec ExecutionContext) error {
	switch evt.Type {
	case "OrderStatusChanged":
		var body struct{ NewStatus string `json:"new_status"` }
		if err := json.Unmarshal(evt.Payload, &body); err != nil { return err }
		return h.repo.UpsertStatus(ctx, evt.OrderID, body.NewStatus)

	case "ShipmentCompanyMetadataReceived":
		var body struct{
			Carrier string         `json:"carrier"`
			Meta    map[string]any `json:"meta"`
		}
		if err := json.Unmarshal(evt.Payload, &body); err != nil { return err }
		if err := h.repo.UpsertShipmentMeta(ctx, evt.OrderID, body.Carrier, body.Meta); err != nil { return err }
		if exec.AllowSideEffects {
			return h.callCarrierWebhook(ctx, evt.OrderID, body.Carrier, body.Meta)
		}
		return nil
	default:
		log.Printf("[UnifiedHandler] unknown event type: %s", evt.Type)
		return nil
	}
}

func (h UnifiedHandler) callCarrierWebhook(ctx context.Context, orderID, carrier string, payload map[string]any) error {
	// In real production, add idempotency keys, signing, retries, etc.
	url := fmt.Sprintf("https://example.com/carriers/%s/orders/%s/webhook", strings.ToLower(carrier), orderID)
	b, _ := json.Marshal(payload)
	log.Printf("[HTTP] POST %s body=%s", url, string(b))
	// This demo logs instead of performing a network call.
	return nil
}

// --- Fan-in processing ----------------------------------------------------------

func processKafkaMessage(ctx context.Context, store EventStore, handler EventHandler, parser Parser, msg KafkaMessage) error {
	// 1) Parse -> canonical OrderEvent
	evt, err := parser.Parse(ctx, msg)
	if err != nil { return err }

	// 2) Persist the event (append-only) for replay
	if err := store.Append(ctx, evt); err != nil { return err }

	// 3) Apply it now to current projection (with side-effects enabled)
	return handler.Handle(ctx, evt, ExecutionContext{AllowSideEffects: true})
}

// --- Replay engine --------------------------------------------------------------

func replayOrder(ctx context.Context, orderID string, store EventStore, handler EventHandler, repo OrderRepository) error {
	log.Printf("
[Replay] rebuilding order %s (side-effects disabled)
", orderID)
	repo.Reset(ctx)
	evts, err := store.ListByOrder(ctx, orderID)
	if err != nil { return err }
	for _, evt := range evts {
		if err := handler.Handle(ctx, evt, ExecutionContext{AllowSideEffects: false}); err != nil { return err }
	}
	return nil
}

// --- Demo -----------------------------------------------------------------------

func main() {
	ctx := context.Background()

	// Infra pieces
	store := NewInMemoryEventStore()
	repo := NewInMemoryOrderRepo()
	h := UnifiedHandler{repo: repo, httpC: &http.Client{Timeout: 3 * time.Second}}

	// Parsers per topic
	parsers := ParserRegistry{
		"order_status_updates":  StatusParser{},
		"shipment_company_meta": ShipmentMetaParser{},
	}

	// Simulate incoming Kafka messages
	now := time.Now()
	messages := []KafkaMessage{
		{Topic: "order_status_updates", Partition: 0, Offset: 1, Timestamp: now.Add(1 * time.Second), Value: mustJSON(map[string]any{"order_id": "A100", "new_status": "CREATED"})},
		{Topic: "shipment_company_meta", Partition: 0, Offset: 2, Timestamp: now.Add(2 * time.Second), Value: mustJSON(map[string]any{"order_id": "A100", "carrier": "DHL", "meta": map[string]any{"zone": "EU", "priority": 2}})},
		{Topic: "order_status_updates", Partition: 0, Offset: 3, Timestamp: now.Add(3 * time.Second), Value: mustJSON(map[string]any{"order_id": "A100", "new_status": "IN_TRANSIT"})},
		{Topic: "order_status_updates", Partition: 0, Offset: 4, Timestamp: now.Add(4 * time.Second), Value: mustJSON(map[string]any{"order_id": "A100", "new_status": "DELIVERED"})},
	}

	for _, m := range messages {
		p := parsers[m.Topic]
		if p == nil { log.Printf("no parser for topic %s", m.Topic); continue }
		if err := processKafkaMessage(ctx, store, h, p, m); err != nil {
			log.Printf("process error: %v", err)
		}
	}

	// Show live state
	if o, ok := repo.Get(ctx, "A100"); ok {
		b, _ := json.MarshalIndent(o, "", "  ")
		fmt.Printf("
LIVE ORDER STATE
%s
", string(b))
	}

	// Replay into a fresh projection (effects OFF)
	if err := replayOrder(ctx, "A100", store, h, repo); err != nil {
		log.Printf("replay error: %v", err)
	}

	// Show rebuilt state
	if o, ok := repo.Get(ctx, "A100"); ok {
		b, _ := json.MarshalIndent(o, "", "  ")
		fmt.Printf("
REBUILT ORDER STATE (from replay)
%s
", string(b))
	}
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
