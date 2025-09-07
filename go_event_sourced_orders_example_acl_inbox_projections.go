# Go Event-Sourced Orders Example

This repository demonstrates an inbound ACL + Inbox driving a domain aggregate and projections. It uses an in‑memory event store and an in‑memory message bus (Kafka abstraction) so you can run it with `go run` and see the full flow end‑to‑end.

---

## How to run

```bash
go run ./cmd/orders-app
```

Then try hitting the (demo) HTTP endpoints:

```
GET http://localhost:8080/orders/demo-1/timeline
GET http://localhost:8080/orders/demo-1/shipment
```

You should see the `AirplaneAllocated` domain event reflected in the projections.

---

## Project layout

```
.
├── cmd/orders-app/main.go
├── go.mod
├── pkg
│   ├── api
│   │   └── httpserver
│   │       └── server.go
│   ├── app
│   │   └── app.go
│   ├── domain
│   │   ├── aggregate
│   │   │   └── order.go
│   │   ├── events.go
│   │   └── meta.go
│   ├── infrastructure
│   │   ├── acl
│   │   │   ├── mapper.go
│   │   │   └── shipment_consumer.go
│   │   ├── bus
│   │   │   └── mem.go
│   │   ├── eventstore
│   │   │   └── memstore.go
│   │   └── inbox
│   │       └── inmemory.go
│   └── projections
│       ├── shipment_details.go
│       └── timeline.go
└── README.md (this file)
```

---

== FILE: go.mod ==
```go
module example.com/ordersapp

go 1.22
```

---

== FILE: cmd/orders-app/main.go ==
```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"example.com/ordersapp/pkg/api/httpserver"
	"example.com/ordersapp/pkg/app"
	"example.com/ordersapp/pkg/domain"
	"example.com/ordersapp/pkg/infrastructure/acl"
	"example.com/ordersapp/pkg/infrastructure/bus"
	"example.com/ordersapp/pkg/infrastructure/eventstore"
	"example.com/ordersapp/pkg/infrastructure/inbox"
	"example.com/ordersapp/pkg/projections"
)

func main() {
	ctx := context.Background()

	// 1) Infra
	msgBus := bus.NewInMemoryBus()
	es := eventstore.NewInMemoryStore()
	inboxRepo := inbox.NewInMemory()

	// 2) Domain app + repo
	repo := app.NewESOrderRepo(es)
	orderApp := app.NewOrderApp(repo, es)

	// 3) Projections subscribe to domain events
	timeline := projections.NewOrderTimeline()
	shipProj := projections.NewShipmentDetails()
	es.Subscribe(func(se eventstore.StoredEvent) {
		// fan-out to projectors; ignore errors in demo
		timeline.Apply(se)
		shipProj.Apply(se)
	})

	// 4) ACL consumer subscribes to external topic and turns facts -> command
	shipmentConsumer := acl.NewShipmentConsumer(inboxRepo, acl.NewShipmentMapper(), orderApp)
	cancel := msgBus.Subscribe("shipment.airplane_loaded", shipmentConsumer.Handle)
	defer cancel()

	// 5) Start a tiny HTTP API to read projections
	server := httpserver.New(":8080", timeline, shipProj)
	go func() {
		if err := server.Start(); err != nil {
			log.Fatal(err)
		}
	}()

	// 6) Publish a demo external event to show the full pipeline working
	publishDemoExternal(msgBus)

	log.Println("server running on http://localhost:8080 … (press Ctrl+C to exit)")
	select {}
}

func publishDemoExternal(bus *bus.InMemoryBus) {
	payload := map[string]any{
		"order_id":   "demo-1",
		"flight_ref": "LY315",
		"airplane": map[string]any{
			"tail": "4X-EHD",
			"type": "B737-800",
			"seats": 189,
		},
		"source_seq":  42,
		"event_time":  time.Now().UTC().Format(time.RFC3339),
	}
	b, _ := json.Marshal(payload)
	msg := bus.Message{
		Topic:     "shipment.airplane_loaded",
		Source:    "shipment",
		ID:        "evt-00042",
		Key:       "demo-1",
		Payload:   b,
		Headers:   map[string]string{"schema": "v1"},
		Partition: 0,
		Offset:    1001,
	}
	_ = bus.Publish(context.Background(), msg)
}
```

---

== FILE: pkg/domain/events.go ==
```go
package domain

import "time"

type Event interface {
	EventType() string
}

// AirplaneAllocated is a domain event emitted by the Order aggregate
// when an airplane has been allocated for this order's shipment.
// Only the decision-critical data lives here.
// NOTE: richer external metadata is placed in event metadata and/or projections.

type AirplaneAllocated struct {
	OrderID    string
	FlightRef  string
	OccurredAt time.Time
}

func (e AirplaneAllocated) EventType() string { return "AirplaneAllocated" }
```

---

== FILE: pkg/domain/meta.go ==
```go
package domain

// Meta is a generic metadata bag attached to stored events.
// Typical keys used in this example:
//   - "source": string (e.g., "shipment")
//   - "source_event_id": string
//   - "source_seq": int64 (monotonic per source)
//   - "schema": string (e.g., "v1")
//   - "payload": any (snapshot of external airplane data)
//   - "at": time.Time (ingestion time)
// Keep it small for hot reads; large blobs should be stored externally and referenced.

type Meta map[string]any
```

---

== FILE: pkg/domain/aggregate/order.go ==
```go
package aggregate

import (
	"time"

	"example.com/ordersapp/pkg/domain"
)

type Order struct {
	ID                string
	Status            string // simplified
	AirplaneAllocated bool
	FlightRef         string

	pending []domain.Event
}

func NewOrder(id string) *Order { return &Order{ID: id} }

// Decision method: record that airplane has been allocated.
// Idempotent: if already allocated, do nothing.
func (o *Order) RecordAirplaneAllocated(flightRef string) {
	if o.AirplaneAllocated && o.FlightRef == flightRef {
		return
	}
	e := domain.AirplaneAllocated{
		OrderID:    o.ID,
		FlightRef:  flightRef,
		OccurredAt: time.Now().UTC(),
	}
	o.apply(e)
	o.pending = append(o.pending, e)
}

func (o *Order) apply(ev domain.Event) {
	switch e := ev.(type) {
	case domain.AirplaneAllocated:
		o.AirplaneAllocated = true
		o.FlightRef = e.FlightRef
	}
}

func (o *Order) ApplyHistory(evs []domain.Event) {
	for _, e := range evs {
		o.apply(e)
	}
}

func (o *Order) DrainPending() []domain.Event {
	p := o.pending
	o.pending = nil
	return p
}
```

---

== FILE: pkg/app/app.go ==
```go
package app

import (
	"context"
	"errors"

	"example.com/ordersapp/pkg/domain"
	"example.com/ordersapp/pkg/domain/aggregate"
	"example.com/ordersapp/pkg/infrastructure/eventstore"
)

type OrderRepository interface {
	Load(ctx context.Context, id string) (*aggregate.Order, error)
}

type esOrderRepo struct{ store *eventstore.InMemoryStore }

func NewESOrderRepo(store *eventstore.InMemoryStore) OrderRepository {
	return &esOrderRepo{store: store}
}

func (r *esOrderRepo) Load(ctx context.Context, id string) (*aggregate.Order, error) {
	evs, err := r.store.LoadAggregate(ctx, id)
	if err != nil {
		return nil, err
	}
	ord := aggregate.NewOrder(id)
	// project history into aggregate state
	var domainEvs []domain.Event
	for _, se := range evs {
		domainEvs = append(domainEvs, se.Event)
	}
	ord.ApplyHistory(domainEvs)
	return ord, nil
}

// OrderApp exposes domain commands used by the ACL.
// In a larger system you'd likely split by use case.

type OrderApp struct {
	repo  OrderRepository
	store *eventstore.InMemoryStore
}

func NewOrderApp(repo OrderRepository, store *eventstore.InMemoryStore) *OrderApp {
	return &OrderApp{repo: repo, store: store}
}

// MarkAirplaneAllocated is a command handler invoked by the inbound ACL.
func (a *OrderApp) MarkAirplaneAllocated(ctx context.Context, orderID, flightRef string, meta domain.Meta) error {
	if orderID == "" || flightRef == "" {
		return errors.New("orderID and flightRef are required")
	}
	ord, err := a.repo.Load(ctx, orderID)
	if err != nil {
		return err
	}
	ord.RecordAirplaneAllocated(flightRef)
	events := ord.DrainPending()
	if len(events) == 0 {
		return nil // idempotent no-op
	}
	return a.store.Append(ctx, orderID, events, meta)
}
```

---

== FILE: pkg/infrastructure/eventstore/memstore.go ==
```go
package eventstore

import (
	"context"
	"sync"
	"time"

	"example.com/ordersapp/pkg/domain"
)

type StoredEvent struct {
	AggregateID string
	Event       domain.Event
	Meta        domain.Meta
	GlobalSeq   int64
}

type InMemoryStore struct {
	mu          sync.Mutex
	byAgg       map[string][]StoredEvent
	global      []StoredEvent
	seq         int64
	subscribers []func(StoredEvent)
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{byAgg: make(map[string][]StoredEvent)}
}

func (s *InMemoryStore) Append(ctx context.Context, aggrID string, events []domain.Event, baseMeta domain.Meta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if baseMeta == nil {
		baseMeta = domain.Meta{}
	}
	if _, ok := baseMeta["at"]; !ok {
		baseMeta["at"] = time.Now().UTC()
	}
	for _, ev := range events {
		s.seq++
		se := StoredEvent{
			AggregateID: aggrID,
			Event:       ev,
			Meta:        cloneMeta(baseMeta),
			GlobalSeq:   s.seq,
		}
		s.byAgg[aggrID] = append(s.byAgg[aggrID], se)
		s.global = append(s.global, se)
		for _, sub := range s.subscribers {
			// notify without holding lock (but we are inside lock). To keep it simple in demo,
			// call under lock; real impl should dispatch async.
			sub(se)
		}
	}
	return nil
}

func cloneMeta(m domain.Meta) domain.Meta {
	c := domain.Meta{}
	for k, v := range m {
		c[k] = v
	}
	return c
}

func (s *InMemoryStore) LoadAggregate(ctx context.Context, aggrID string) ([]StoredEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev := s.byAgg[aggrID]
	// return a shallow copy to avoid external mutation
	out := make([]StoredEvent, len(ev))
	copy(out, ev)
	return out, nil
}

func (s *InMemoryStore) Subscribe(cb func(StoredEvent)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscribers = append(s.subscribers, cb)
}
```

---

== FILE: pkg/infrastructure/inbox/inmemory.go ==
```go
package inbox

import "sync"

type status uint8

const (
	seen status = iota + 1
	processed
	failed
)

type record struct {
	st status
	err string
}

type InMemory struct{
	mu sync.Mutex
	m  map[string]record // key = source + "|" + id
}

func NewInMemory() *InMemory { return &InMemory{m: make(map[string]record)} }

func (i *InMemory) key(source, id string) string { return source + "|" + id }

func (i *InMemory) Seen(source, id string) bool {
	i.mu.Lock(); defer i.mu.Unlock()
	_, ok := i.m[i.key(source, id)]
	return ok
}

func (i *InMemory) MarkSeen(source, id string) {
	i.mu.Lock(); defer i.mu.Unlock()
	i.m[i.key(source, id)] = record{st: seen}
}

func (i *InMemory) MarkProcessed(source, id string) {
	i.mu.Lock(); defer i.mu.Unlock()
	i.m[i.key(source, id)] = record{st: processed}
}

func (i *InMemory) MarkFailed(source, id, err string) {
	i.mu.Lock(); defer i.mu.Unlock()
	i.m[i.key(source, id)] = record{st: failed, err: err}
}
```

---

== FILE: pkg/infrastructure/bus/mem.go ==
```go
package bus

import (
	"context"
	"sync"
)

type Message struct {
	Topic     string
	Source    string
	ID        string
	Key       string
	Payload   []byte
	Headers   map[string]string
	Partition int
	Offset    int64
}

type handler func(context.Context, Message) error

type InMemoryBus struct {
	mu        sync.RWMutex
	handlers  map[string][]handler
}

func NewInMemoryBus() *InMemoryBus { return &InMemoryBus{handlers: make(map[string][]handler)} }

func (b *InMemoryBus) Publish(ctx context.Context, msg Message) error {
	b.mu.RLock()
	hs := append([]handler(nil), b.handlers[msg.Topic]...)
	b.mu.RUnlock()
	for _, h := range hs {
		// fan out sequentially for demo simplicity
		_ = h(ctx, msg)
	}
	return nil
}

// Subscribe registers a handler for a topic and returns a cancel func.
func (b *InMemoryBus) Subscribe(topic string, h func(context.Context, Message) error) (cancel func()) {
	b.mu.Lock()
	b.handlers[topic] = append(b.handlers[topic], h)
	idx := len(b.handlers[topic]) - 1
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		hs := b.handlers[topic]
		if idx >= 0 && idx < len(hs) {
			b.handlers[topic] = append(hs[:idx], hs[idx+1:]...)
		}
	}
}
```

---

== FILE: pkg/infrastructure/acl/mapper.go ==
```go
package acl

import (
	"encoding/json"
	"time"

	"example.com/ordersapp/pkg/infrastructure/bus"
)

// CanonicalShipmentLoaded is the canonical form we map external payloads into.
// Only the fields our domain needs directly are surfaced; the rest remains in airplane.

type CanonicalShipmentLoaded struct {
	OrderID     string
	FlightRef   string
	Airplane    map[string]any
	SourceSeq   int64
	EventTime   time.Time
}

type ShipmentMapper struct{}

func NewShipmentMapper() *ShipmentMapper { return &ShipmentMapper{} }

func (m *ShipmentMapper) Map(msg bus.Message) (CanonicalShipmentLoaded, error) {
	var raw struct {
		OrderID   string         `json:"order_id"`
		FlightRef string         `json:"flight_ref"`
		Airplane  map[string]any `json:"airplane"`
		SourceSeq int64          `json:"source_seq"`
		EventTime string         `json:"event_time"`
	}
	if err := json.Unmarshal(msg.Payload, &raw); err != nil {
		return CanonicalShipmentLoaded{}, err
	}
	et, _ := time.Parse(time.RFC3339, raw.EventTime)
	return CanonicalShipmentLoaded{
		OrderID:   raw.OrderID,
		FlightRef: raw.FlightRef,
		Airplane:  raw.Airplane,
		SourceSeq: raw.SourceSeq,
		EventTime: et,
	}, nil
}
```

---

== FILE: pkg/infrastructure/acl/shipment_consumer.go ==
```go
package acl

import (
	"context"
	"fmt"
	"log"

	"example.com/ordersapp/pkg/app"
	"example.com/ordersapp/pkg/domain"
	"example.com/ordersapp/pkg/infrastructure/bus"
	"example.com/ordersapp/pkg/infrastructure/inbox"
)

type ShipmentConsumer struct {
	inbox  *inbox.InMemory
	mapper *ShipmentMapper
	app    *app.OrderApp
}

func NewShipmentConsumer(inbox *inbox.InMemory, mapper *ShipmentMapper, app *app.OrderApp) *ShipmentConsumer {
	return &ShipmentConsumer{inbox: inbox, mapper: mapper, app: app}
}

func (c *ShipmentConsumer) Handle(ctx context.Context, msg bus.Message) error {
	key := fmt.Sprintf("%s|%s", msg.Source, msg.ID)
	if c.inbox.Seen(msg.Source, msg.ID) {
		log.Printf("inbox: duplicate message %s, skipping", key)
		return nil
	}
	c.inbox.MarkSeen(msg.Source, msg.ID)

	canon, err := c.mapper.Map(msg)
	if err != nil {
		c.inbox.MarkFailed(msg.Source, msg.ID, err.Error())
		return err
	}

	meta := domain.Meta{
		"source":          msg.Source,
		"source_event_id": msg.ID,
		"schema":          msg.Headers["schema"],
		"source_seq":      canon.SourceSeq,
		"payload":         canon.Airplane, // snapshot of external airplane data
	}

	if err := c.app.MarkAirplaneAllocated(ctx, canon.OrderID, canon.FlightRef, meta); err != nil {
		c.inbox.MarkFailed(msg.Source, msg.ID, err.Error())
		return err
	}
	c.inbox.MarkProcessed(msg.Source, msg.ID)
	return nil
}
```

---

== FILE: pkg/projections/timeline.go ==
```go
package projections

import (
	"sync"

	"example.com/ordersapp/pkg/infrastructure/eventstore"
)

type TimelineItem struct {
	Type string      `json:"type"`
	At   any         `json:"at"`
	Data interface{} `json:"data"`
}

type OrderTimeline struct {
	mu   sync.RWMutex
	data map[string][]TimelineItem // orderID -> items
}

func NewOrderTimeline() *OrderTimeline {
	return &OrderTimeline{data: make(map[string][]TimelineItem)}
}

func (p *OrderTimeline) Apply(se eventstore.StoredEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	id := se.AggregateID
	p.data[id] = append(p.data[id], TimelineItem{
		Type: se.Event.EventType(),
		At:   se.Meta["at"],
		Data: se.Event,
	})
}

func (p *OrderTimeline) Get(orderID string) []TimelineItem {
	p.mu.RLock(); defer p.mu.RUnlock()
	items := p.data[orderID]
	out := make([]TimelineItem, len(items))
	copy(out, items)
	return out
}
```

---

== FILE: pkg/projections/shipment_details.go ==
```go
package projections

import (
	"sync"

	"example.com/ordersapp/pkg/domain"
	"example.com/ordersapp/pkg/infrastructure/eventstore"
)

type ShipmentDetails struct {
	OrderID   string         `json:"order_id"`
	FlightRef string         `json:"flight_ref"`
	Airplane  map[string]any `json:"airplane"`
	Version   int64          `json:"version"` // from source_seq
}

type ShipmentDetailsProjection struct {
	mu   sync.RWMutex
	data map[string]ShipmentDetails // orderID -> current snapshot
}

func NewShipmentDetails() *ShipmentDetailsProjection {
	return &ShipmentDetailsProjection{data: make(map[string]ShipmentDetails)}
}

func (p *ShipmentDetailsProjection) Apply(se eventstore.StoredEvent) {
	switch ev := se.Event.(type) {
	case domain.AirplaneAllocated:
		p.mu.Lock()
		defer p.mu.Unlock()
		cur := p.data[ev.OrderID]
		var seq int64
		if v, ok := se.Meta["source_seq"].(int64); ok { seq = v }
		if seq < cur.Version { // out-of-order older update; ignore
			return
		}
		plane := map[string]any{}
		if m, ok := se.Meta["payload"].(map[string]any); ok { plane = m }
		p.data[ev.OrderID] = ShipmentDetails{
			OrderID:   ev.OrderID,
			FlightRef: ev.FlightRef,
			Airplane:  plane,
			Version:   seq,
		}
	}
}

func (p *ShipmentDetailsProjection) Get(orderID string) (ShipmentDetails, bool) {
	p.mu.RLock(); defer p.mu.RUnlock()
	res, ok := p.data[orderID]
	return res, ok
}
```

---

== FILE: pkg/api/httpserver/server.go ==
```go
package httpserver

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"example.com/ordersapp/pkg/projections"
)

type Server struct {
	addr      string
	timeline  *projections.OrderTimeline
	shipProj  *projections.ShipmentDetailsProjection
}

func New(addr string, timeline *projections.OrderTimeline, ship *projections.ShipmentDetailsProjection) *Server {
	return &Server{addr: addr, timeline: timeline, shipProj: ship}
}

func (s *Server) Start() error {
	http.HandleFunc("/orders/", s.handleOrders)
	log.Printf("HTTP listening on %s", s.addr)
	return http.ListenAndServe(s.addr, nil)
}

func (s *Server) handleOrders(w http.ResponseWriter, r *http.Request) {
	// crude router: /orders/{id}/timeline or /orders/{id}/shipment
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/orders/"), "/")
	if len(parts) < 2 { http.NotFound(w, r); return }
	id := parts[0]
	action := parts[1]
	switch action {
	case "timeline":
		items := s.timeline.Get(id)
		writeJSON(w, items)
	case "shipment":
		res, ok := s.shipProj.Get(id)
		if !ok { http.NotFound(w, r); return }
		writeJSON(w, res)
	default:
		http.NotFound(w, r)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
```

---

## Notes & next steps
- Swap `pkg/infrastructure/bus/mem.go` with a real Kafka consumer (e.g., confluent-kafka-go) behind the same interface.
- Replace the Inbox with a durable store (SQL) keyed by `(source, event_id)` or `(topic, partition, offset)`.
- Move `Subscribe` callbacks in `memstore` to async workers and add error handling + retries for projectors.
- Emit additional domain events for other integration facts; keep aggregates minimal and push rich metadata into projections.
- Add an Outbox if you later need to publish *outgoing* domain events to Kafka.
