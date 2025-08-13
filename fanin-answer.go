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

// --- Kafka-like message (minimal) -------------------------------------------------

type KafkaMessage struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

func msgID(m KafkaMessage) string { return fmt.Sprintf("%s-%d-%d", m.Topic, m.Partition, m.Offset) }

// --- Domain Events ---------------------------------------------------------------

type DomainEvent interface {
	Type() string
	OrderID() string
}

type OrderStatusChanged struct {
	OrderIDVal string
	NewStatus  string
}

func (e OrderStatusChanged) Type() string  { return "OrderStatusChanged" }
func (e OrderStatusChanged) OrderID() string { return e.OrderIDVal }

type ShipmentCompanyMetadataReceived struct {
	OrderIDVal string
	Carrier    string
	Meta       map[string]any
}

func (e ShipmentCompanyMetadataReceived) Type() string  { return "ShipmentCompanyMetadataReceived" }
func (e ShipmentCompanyMetadataReceived) OrderID() string { return e.OrderIDVal }

// --- Commands -------------------------------------------------------------------

type Command interface { Name() string }

type SaveShipmentMeta struct {
	OrderID string
	Carrier string
	Meta    map[string]any
}

func (c SaveShipmentMeta) Name() string { return "SaveShipmentMeta" }

type CallCarrierWebhook struct {
	OrderID string
	Carrier string
	Payload map[string]any
}

func (c CallCarrierWebhook) Name() string { return "CallCarrierWebhook" }

// --- Parsers (Fan-In: topic -> parser) ------------------------------------------

type Parser interface {
	Parse(ctx context.Context, msg KafkaMessage) ([]DomainEvent, error)
}

type ParserRegistry map[string]Parser // topic -> parser

// Example parser for order status updates
// Topic: order_status_updates
// Payload: {"order_id":"123","new_status":"IN_TRANSIT"}

type StatusParser struct{}

func (StatusParser) Parse(ctx context.Context, msg KafkaMessage) ([]DomainEvent, error) {
	var p struct {
		OrderID   string `json:"order_id"`
		NewStatus string `json:"new_status"`
	}
	if err := json.Unmarshal(msg.Value, &p); err != nil {
		return nil, fmt.Errorf("status parse: %w", err)
	}
	if p.OrderID == "" || p.NewStatus == "" {
		return nil, errors.New("status parse: missing fields")
	}
	return []DomainEvent{OrderStatusChanged{OrderIDVal: p.OrderID, NewStatus: p.NewStatus}}, nil
}

// Example parser for shipment-company metadata
// Topic: shipment_company_meta
// Payload: {"order_id":"123","carrier":"DHL","meta":{"zone":"EU","priority":2}}

type ShipmentMetaParser struct{}

func (ShipmentMetaParser) Parse(ctx context.Context, msg KafkaMessage) ([]DomainEvent, error) {
	var p struct {
		OrderID string         `json:"order_id"`
		Carrier string         `json:"carrier"`
		Meta    map[string]any `json:"meta"`
	}
	if err := json.Unmarshal(msg.Value, &p); err != nil {
		return nil, fmt.Errorf("shipment meta parse: %w", err)
	}
	if p.OrderID == "" || p.Carrier == "" { return nil, errors.New("shipment meta parse: missing fields") }
	return []DomainEvent{ShipmentCompanyMetadataReceived{OrderIDVal: p.OrderID, Carrier: p.Carrier, Meta: p.Meta}}, nil
}

// --- Event Router & Handlers -----------------------------------------------------

type EventHandler interface {
	Handle(ctx context.Context, evt DomainEvent) ([]Command, error) // can emit commands
}

type Router struct { handlers map[string][]EventHandler }

func NewRouter() *Router { return &Router{handlers: map[string][]EventHandler{}} }

func (r *Router) Register(eventType string, h EventHandler) {
	r.handlers[eventType] = append(r.handlers[eventType], h)
}

func (r *Router) Dispatch(ctx context.Context, evt DomainEvent) ([]Command, error) {
	hs := r.handlers[evt.Type()]
	var cmds []Command
	for _, h := range hs {
		c, err := h.Handle(ctx, evt)
		if err != nil { return nil, err }
		cmds = append(cmds, c...)
	}
	return cmds, nil
}

// --- Repositories & Unit of Work -------------------------------------------------

type Order struct {
	ID         string
	Status     string
	ShipMeta   map[string]map[string]any // carrier -> meta
}

type OrderRepository interface {
	UpdateStatus(ctx context.Context, orderID, newStatus string) error
	UpsertShipmentMeta(ctx context.Context, orderID, carrier string, meta map[string]any) error
	Get(ctx context.Context, orderID string) (Order, bool)
}

type InMemoryOrderRepo struct { mu sync.Mutex; store map[string]Order }

func NewInMemoryOrderRepo() *InMemoryOrderRepo { return &InMemoryOrderRepo{store: map[string]Order{}} }

func (r *InMemoryOrderRepo) UpdateStatus(ctx context.Context, orderID, newStatus string) error {
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
	// shallow copy for safety
	copied := map[string]any{}
	for k, v := range meta { copied[k] = v }
	o.ShipMeta[carrier] = copied
	r.store[orderID] = o
	log.Printf("[OrderRepo] order %s carrier %s meta upserted: %v", orderID, carrier, meta)
	return nil
}

func (r *InMemoryOrderRepo) Get(ctx context.Context, orderID string) (Order, bool) {
	r.mu.Lock(); defer r.mu.Unlock()
	o, ok := r.store[orderID]
	return o, ok
}

// Unit of Work (naive in-memory). In real life, use DB tx.

type UnitOfWork interface { Do(ctx context.Context, fn func(ctx context.Context) error) error }

type InMemoryUoW struct{ mu sync.Mutex }

func (u *InMemoryUoW) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	u.mu.Lock(); defer u.mu.Unlock()
	return fn(ctx)
}

// Idempotency store by message id

type DedupeRepo interface { Seen(id string) bool; MarkSeen(id string) }

type InMemoryDedupe struct{ mu sync.Mutex; seen map[string]struct{} }

func NewInMemoryDedupe() *InMemoryDedupe { return &InMemoryDedupe{seen: map[string]struct{}{}} }

func (d *InMemoryDedupe) Seen(id string) bool { d.mu.Lock(); defer d.mu.Unlock(); _, ok := d.seen[id]; return ok }
func (d *InMemoryDedupe) MarkSeen(id string) { d.mu.Lock(); defer d.mu.Unlock(); d.seen[id] = struct{}{} }

// --- Outbox ---------------------------------------------------------------------

type OutboxRecord struct {
	ID           int64
	EnqueuedAt   time.Time
	Attempts     int
	NextAttempt  time.Time
	Status       string // pending|done|failed
	Cmd          Command
}

type OutboxRepo interface {
	Enqueue(ctx context.Context, cmds ...Command) error
	DequeueBatch(ctx context.Context, n int) ([]OutboxRecord, error)
	MarkDone(ctx context.Context, id int64) error
	MarkRetry(ctx context.Context, id int64, after time.Duration) error
}

type InMemoryOutbox struct { mu sync.Mutex; seq int64; q []*OutboxRecord }

func NewInMemoryOutbox() *InMemoryOutbox { return &InMemoryOutbox{} }

func (o *InMemoryOutbox) Enqueue(ctx context.Context, cmds ...Command) error {
	o.mu.Lock(); defer o.mu.Unlock()
	for _, c := range cmds {
		o.seq++
		rec := &OutboxRecord{ID: o.seq, EnqueuedAt: time.Now(), NextAttempt: time.Now(), Status: "pending", Cmd: c}
		o.q = append(o.q, rec)
		log.Printf("[Outbox] enqueued cmd #%d %s", rec.ID, c.Name())
	}
	return nil
}

func (o *InMemoryOutbox) DequeueBatch(ctx context.Context, n int) ([]OutboxRecord, error) {
	o.mu.Lock(); defer o.mu.Unlock()
	now := time.Now()
	var picked []OutboxRecord
	for _, rec := range o.q {
		if rec.Status == "pending" && !rec.NextAttempt.After(now) {
			picked = append(picked, *rec)
			if len(picked) >= n { break }
		}
	}
	return picked, nil
}

func (o *InMemoryOutbox) MarkDone(ctx context.Context, id int64) error {
	o.mu.Lock(); defer o.mu.Unlock()
	for _, rec := range o.q { if rec.ID == id { rec.Status = "done"; return nil } }
	return fmt.Errorf("outbox: id %d not found", id)
}

func (o *InMemoryOutbox) MarkRetry(ctx context.Context, id int64, after time.Duration) error {
	o.mu.Lock(); defer o.mu.Unlock()
	for _, rec := range o.q {
		if rec.ID == id {
			rec.Attempts++
			rec.NextAttempt = time.Now().Add(after)
			log.Printf("[Outbox] retry #%d for cmd %d in %v", rec.Attempts, rec.ID, after)
			if rec.Attempts > 10 { rec.Status = "failed" }
			return nil
		}
	}
	return fmt.Errorf("outbox: id %d not found", id)
}

// --- Command Handling ------------------------------------------------------------

type CommandHandler interface {
	CanHandle(cmd Command) bool
	Handle(ctx context.Context, cmd Command) error
}

type RepoCommandHandler struct{ repo OrderRepository }

func (h RepoCommandHandler) CanHandle(cmd Command) bool { _, ok := cmd.(SaveShipmentMeta); return ok }

func (h RepoCommandHandler) Handle(ctx context.Context, cmd Command) error {
	c := cmd.(SaveShipmentMeta)
	return h.repo.UpsertShipmentMeta(ctx, c.OrderID, c.Carrier, c.Meta)
}

type HTTPWebhookHandler struct{ Client *http.Client }

func (h HTTPWebhookHandler) CanHandle(cmd Command) bool { _, ok := cmd.(CallCarrierWebhook); return ok }

func (h HTTPWebhookHandler) Handle(ctx context.Context, cmd Command) error {
	c := cmd.(CallCarrierWebhook)
	// Simulate calling carrier webhook. In real life, sign requests & use idempotency keys.
	url := fmt.Sprintf("https://example.com/carriers/%s/orders/%s/webhook", strings.ToLower(c.Carrier), c.OrderID)
	body, _ := json.Marshal(c.Payload)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	// Don't actually send over the network in this demo. Just log.
	log.Printf("[HTTP] POST %s body=%s", url, string(body))
	return nil
}

// Outbox worker that executes commands with retries/backoff

type OutboxWorker struct {
	ob       OutboxRepo
	handlers []CommandHandler
	batch    int
	backoff  func(attempt int) time.Duration
}

func (w *OutboxWorker) Start(ctx context.Context) {
	log.Printf("[Worker] started")
	for {
		select {
		case <-ctx.Done():
			log.Printf("[Worker] stopping: %v", ctx.Err())
			return
		default:
		}
		recs, _ := w.ob.DequeueBatch(ctx, w.batch)
		if len(recs) == 0 { time.Sleep(200 * time.Millisecond); continue }
		for _, r := range recs {
			var handled bool
			for _, h := range w.handlers {
				if h.CanHandle(r.Cmd) {
					if err := h.Handle(ctx, r.Cmd); err != nil {
						log.Printf("[Worker] cmd %d %s failed: %v", r.ID, r.Cmd.Name(), err)
						// naive backoff by attempts in record (we don't have it here, so use 1s)
						_ = w.ob.MarkRetry(ctx, r.ID, 2*time.Second)
					} else {
						log.Printf("[Worker] cmd %d %s done", r.ID, r.Cmd.Name())
						_ = w.ob.MarkDone(ctx, r.ID)
					}
					handled = true
					break
				}
			}
			if !handled {
				log.Printf("[Worker] no handler for cmd %d %s", r.ID, r.Cmd.Name())
				_ = w.ob.MarkRetry(ctx, r.ID, 5*time.Second)
			}
		}
	}
}

// --- Handlers: Projection & Policy ----------------------------------------------

type OrderProjectionHandler struct{ repo OrderRepository }

func (h OrderProjectionHandler) Handle(ctx context.Context, evt DomainEvent) ([]Command, error) {
	switch e := evt.(type) {
	case OrderStatusChanged:
		return nil, h.repo.UpdateStatus(ctx, e.OrderID(), e.NewStatus)
	default:
		return nil, nil
	}
}

type ShipmentMetaPolicy struct{}

func (ShipmentMetaPolicy) Handle(ctx context.Context, evt DomainEvent) ([]Command, error) {
	e, ok := evt.(ShipmentCompanyMetadataReceived)
	if !ok { return nil, nil }
	cmds := []Command{
		SaveShipmentMeta{OrderID: e.OrderID(), Carrier: e.Carrier, Meta: e.Meta},
		CallCarrierWebhook{OrderID: e.OrderID(), Carrier: e.Carrier, Payload: e.Meta},
	}
	return cmds, nil
}

// --- Processing loop -------------------------------------------------------------

func processKafkaMessage(ctx context.Context, uow UnitOfWork, dedupe DedupeRepo, p Parser, r *Router, out OutboxRepo, msg KafkaMessage) error {
	id := msgID(msg)
	if dedupe.Seen(id) {
		log.Printf("[Ingest] skip duplicate %s", id)
		return nil
	}
	return uow.Do(ctx, func(tx context.Context) error {
		evts, err := p.Parse(tx, msg)
		if err != nil { return err }
		var allCmds []Command
		for _, evt := range evts {
			log.Printf("[Ingest] evt %s for order %s", evt.Type(), evt.OrderID())
			cmds, err := r.Dispatch(tx, evt)
			if err != nil { return err }
			allCmds = append(allCmds, cmds...)
		}
		if len(allCmds) > 0 {
			if err := out.Enqueue(tx, allCmds...); err != nil { return err }
		}
		dedupe.MarkSeen(id)
		return nil
	})
}

// --- Demo ------------------------------------------------------------------------

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Infra
	repo := NewInMemoryOrderRepo()
	uow := &InMemoryUoW{}
	dedupe := NewInMemoryDedupe()
	outbox := NewInMemoryOutbox()

	// Router & Handlers
	router := NewRouter()
	router.Register("OrderStatusChanged", OrderProjectionHandler{repo: repo})
	router.Register("ShipmentCompanyMetadataReceived", ShipmentMetaPolicy{})

	// Command execution (outbox worker)
	worker := &OutboxWorker{
		ob: outbox,
		handlers: []CommandHandler{
			RepoCommandHandler{repo: repo},
			HTTPWebhookHandler{Client: &http.Client{Timeout: 3 * time.Second}},
		},
		batch: 10,
	}
	go worker.Start(ctx)

	// Parsers per topic (Fan-In)
	parsers := ParserRegistry{
		"order_status_updates":  StatusParser{},
		"shipment_company_meta": ShipmentMetaParser{},
	}

	// Simulate messages from multiple topics
	messages := []KafkaMessage{
		{Topic: "order_status_updates", Partition: 0, Offset: 1, Value: mustJSON(map[string]any{"order_id": "A100", "new_status": "CREATED"})},
		{Topic: "shipment_company_meta", Partition: 0, Offset: 2, Value: mustJSON(map[string]any{"order_id": "A100", "carrier": "DHL", "meta": map[string]any{"zone": "EU", "priority": 2}})},
		{Topic: "order_status_updates", Partition: 0, Offset: 3, Value: mustJSON(map[string]any{"order_id": "A100", "new_status": "IN_TRANSIT"})},
		{Topic: "order_status_updates", Partition: 0, Offset: 4, Value: mustJSON(map[string]any{"order_id": "A100", "new_status": "DELIVERED"})},
	}

	// Fan-in loop
	for _, m := range messages {
		p := parsers[m.Topic]
		if p == nil { log.Printf("no parser for topic %s", m.Topic); continue }
		if err := processKafkaMessage(ctx, uow, dedupe, p, router, outbox, m); err != nil {
			log.Printf("process error: %v", err)
		}
	}

	// Give worker time to drain the outbox in this demo
	time.Sleep(2 * time.Second)

	// Show final state
	if o, ok := repo.Get(ctx, "A100"); ok {
		b, _ := json.MarshalIndent(o, "", "  ")
		fmt.Printf("\nFINAL ORDER STATE\n%s\n", string(b))
	}
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
