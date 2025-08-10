Love it. Let’s add **sub-steps** so each step can have its own nested mini-machines—still per-order, still event-driven, and still idempotent.

I’ll show:

* Mongo shape (with substeps + inheritance policy)
* Go types
* Engine changes: snapshot → evaluate (parent + substeps) → apply → (optionally) derive parent status from substeps

---

# Mongo document (per order)

```json
{
  "_id": "ORD_123",
  "steps": [
    {
      "name": "Billing",
      "status": "not_started",
      "machine": {
        "onEvent": {
          "invoice_created": { "to": "in_progress" },
          "payment_cleared": { "to": "done" }
        }
      },
      "substeps": [
        {
          "name": "Issue Invoice",
          "status": "not_started",
          "machine": {
            "onEvent": {
              "invoice_created": { "to": "done" }
            }
          }
        },
        {
          "name": "Collect Payment",
          "status": "not_started",
          "machine": {
            "onEvent": {
              "payment_cleared": { "to": "done" }
            }
          }
        }
      ],
      "aggregation": { "deriveParentFromSubsteps": true }
    },
    {
      "name": "Customs",
      "status": "not_started",
      "machine": {
        "onEvent": {
          "payment_cleared": { "to": "in_progress" },
          "customs_approved": {
            "to": "done",
            "require": [{ "step": "Billing", "status": "done" }]
          }
        }
      },
      "substeps": [
        {
          "name": "Docs Check",
          "status": "not_started",
          "machine": {
            "onEvent": {
              "payment_cleared": { "to": "in_progress" },
              "customs_approved": { "to": "done" }
            }
          }
        },
        {
          "name": "Fees",
          "status": "not_started",
          "machine": {
            "onEvent": { "customs_approved": { "to": "done" } }
          }
        }
      ],
      "aggregation": { "deriveParentFromSubsteps": true }
    }
  ],
  "policy": { "forwardOnly": true },
  "version": 8,
  "updatedAt": "2025-08-10T11:32:10Z"
}
```

* Each **step** can have its own **substeps\[]**, each with its own `machine.onEvent`.
* `aggregation.deriveParentFromSubsteps = true` means the step’s status is **computed** from substeps (rules below). If false/missing, the step may also have its own transitions independent of substeps.

**Parent status derivation (simple, predictable):**

1. If **all substeps == done** → parent = `done`
2. Else if **any substep == in\_progress OR done** → parent = `in_progress`
3. Else → parent = `not_started`
   (You can tighten this—e.g., require *all in\_progress or done* to be `in_progress`—but the above is intuitive.)

---

# Go types (add Substeps + Aggregation)

```go
type StepStatus string

const (
	StatusNotStarted StepStatus = "not_started"
	StatusInProgress StepStatus = "in_progress"
	StatusDone       StepStatus = "done"
)

type GuardReq struct {
	Step     string     `bson:"step" json:"step"`               // parent step name
	Substep  string     `bson:"substep,omitempty" json:"substep,omitempty"` // optional: target a substep
	Status   StepStatus `bson:"status" json:"status"`
}

type StepTransition struct {
	To      StepStatus `bson:"to" json:"to"`
	Require []GuardReq `bson:"require,omitempty" json:"require,omitempty"`
}

type StepMachine struct {
	OnEvent map[string]StepTransition `bson:"onEvent" json:"onEvent"`
}

type Aggregation struct {
	DeriveParentFromSubsteps bool `bson:"deriveParentFromSubsteps" json:"deriveParentFromSubsteps"`
}

type Substep struct {
	Name        string       `bson:"name" json:"name"`
	Status      StepStatus   `bson:"status" json:"status"`
	StartedAt   *time.Time   `bson:"startedAt,omitempty" json:"startedAt,omitempty"`
	CompletedAt *time.Time   `bson:"completedAt,omitempty" json:"completedAt,omitempty"`
	Machine     *StepMachine `bson:"machine,omitempty" json:"machine,omitempty"`
	Position    *int         `bson:"position,omitempty" json:"position,omitempty"` // optional for UI ordering
}

type Step struct {
	Name        string       `bson:"name" json:"name"`
	Status      StepStatus   `bson:"status" json:"status"`
	StartedAt   *time.Time   `bson:"startedAt,omitempty" json:"startedAt,omitempty"`
	CompletedAt *time.Time   `bson:"completedAt,omitempty" json:"completedAt,omitempty"`
	Machine     *StepMachine `bson:"machine,omitempty" json:"machine,omitempty"`
	Substeps    []Substep    `bson:"substeps,omitempty" json:"substeps,omitempty"`
	Aggregation *Aggregation `bson:"aggregation,omitempty" json:"aggregation,omitempty"`
	Position    *int         `bson:"position,omitempty" json:"position,omitempty"`
}

type Policy struct {
	ForwardOnly bool `bson:"forwardOnly" json:"forwardOnly"`
}

type Order struct {
	ID        string    `bson:"_id" json:"id"`
	Steps     []Step    `bson:"steps" json:"steps"`
	Policy    Policy    `bson:"policy,omitempty" json:"policy,omitempty"`
	Version   int64     `bson:"version" json:"version"`
	UpdatedAt time.Time `bson:"updatedAt" json:"updatedAt"`
}

type Event struct {
	OrderID    string
	Name       string    // e.g., "payment_cleared"
	EventID    string
	ReceivedAt time.Time
}
```

---

# Engine changes

### Snapshot structure

We need guards to reference **steps OR substeps**. Build a snapshot map for both.

```go
type Engine struct{ col *mongo.Collection }

func (e *Engine) ApplyEvent(ctx context.Context, ev Event) error {
	var ord Order
	if err := e.col.FindOne(ctx, bson.M{"_id": ev.OrderID}).Decode(&ord); err != nil {
		return err
	}
	now := ev.ReceivedAt

	// 1) Pre-event snapshot
	stepStatus := make(map[string]StepStatus, len(ord.Steps))
	subStatus := map[string]map[string]StepStatus{} // step -> substep -> status

	for i := range ord.Steps {
		s := &ord.Steps[i]
		key := strings.ToLower(s.Name)
		stepStatus[key] = s.Status
		if len(s.Substeps) > 0 {
			ss := make(map[string]StepStatus, len(s.Substeps))
			for j := range s.Substeps {
				ss[strings.ToLower(s.Substeps[j].Name)] = s.Substeps[j].Status
			}
			subStatus[key] = ss
		}
	}

	changed := false

	// 2) Evaluate parent steps against event (without mutating snapshot)
	for i := range ord.Steps {
		step := &ord.Steps[i]
		// (a) Parent reacts?
		if step.Machine != nil && step.Machine.OnEvent != nil {
			if tr, ok := step.Machine.OnEvent[ev.Name]; ok {
				if guardsOK(tr.Require, stepStatus, subStatus) {
					if applyTransitionStep(step, tr.To, ord.Policy.ForwardOnly, now) {
						changed = true
					}
				}
			}
		}
		// (b) Substeps react?
		for j := range step.Substeps {
			sub := &step.Substeps[j]
			if sub.Machine == nil || sub.Machine.OnEvent == nil {
				continue
			}
			if tr, ok := sub.Machine.OnEvent[ev.Name]; ok {
				if guardsOK(tr.Require, stepStatus, subStatus) {
					if applyTransitionSubstep(sub, tr.To, ord.Policy.ForwardOnly, now) {
						changed = true
					}
				}
			}
		}
	}

	// 3) Optional aggregation: derive parent status from its substeps
	for i := range ord.Steps {
		step := &ord.Steps[i]
		if step.Aggregation != nil && step.Aggregation.DeriveParentFromSubsteps && len(step.Substeps) > 0 {
			newParent := aggregateFromSubsteps(step.Substeps)
			// Only allow forward move if policy requires it
			if step.Status != newParent && (!ord.Policy.ForwardOnly || canGoForward(step.Status, newParent)) {
				applyTimestampsForParent(step, newParent, now)
				step.Status = newParent
				changed = true
			}
		}
	}

	if !changed {
		return nil // or return ErrNoEffect
	}

	// 4) Persist with OCC
	prev := ord.Version
	ord.Version++
	ord.UpdatedAt = time.Now().UTC()

	res, err := e.col.UpdateOne(
		ctx,
		bson.M{"_id": ord.ID, "version": prev},
		bson.M{"$set": bson.M{
			"steps":     ord.Steps,
			"version":   ord.Version,
			"updatedAt": ord.UpdatedAt,
		}},
	)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		// retry once (or backoff)
		return e.ApplyEvent(ctx, ev)
	}
	return nil
}

func guardsOK(reqs []GuardReq, stepStatus map[string]StepStatus, subStatus map[string]map[string]StepStatus) bool {
	for _, r := range reqs {
		stepKey := strings.ToLower(r.Step)
		if r.Substep == "" {
			if stepStatus[stepKey] != r.Status {
				return false
			}
		} else {
			subKey := strings.ToLower(r.Substep)
			if subStatus[stepKey] == nil || subStatus[stepKey][subKey] != r.Status {
				return false
			}
		}
	}
	return true
}

func applyTransitionStep(s *Step, to StepStatus, forwardOnly bool, now time.Time) bool {
	if s.Status == to {
		return false
	}
	if forwardOnly && !canGoForward(s.Status, to) {
		return false
	}
	applyTimestampsForParent(s, to, now)
	s.Status = to
	return true
}

func applyTransitionSubstep(s *Substep, to StepStatus, forwardOnly bool, now time.Time) bool {
	if s.Status == to {
		return false
	}
	if forwardOnly && !canGoForward(s.Status, to) {
		return false
	}
	switch to {
	case StatusInProgress:
		if s.StartedAt == nil {
			t := now
			s.StartedAt = &t
		}
		s.CompletedAt = nil
	case StatusDone:
		if s.StartedAt == nil {
			t := now
			s.StartedAt = &t
		}
		t := now
		s.CompletedAt = &t
	case StatusNotStarted:
		s.StartedAt, s.CompletedAt = nil, nil
	}
	s.Status = to
	return true
}

func applyTimestampsForParent(s *Step, to StepStatus, now time.Time) {
	switch to {
	case StatusInProgress:
		if s.StartedAt == nil {
			t := now
			s.StartedAt = &t
		}
		s.CompletedAt = nil
	case StatusDone:
		if s.StartedAt == nil {
			t := now
			s.StartedAt = &t
		}
		t := now
		s.CompletedAt = &t
	case StatusNotStarted:
		s.StartedAt, s.CompletedAt = nil, nil
	}
}

func aggregateFromSubsteps(subs []Substep) StepStatus {
	allDone := true
	anyActive := false
	for _, ss := range subs {
		switch ss.Status {
		case StatusDone:
			anyActive = true // counts toward "progress made"
		case StatusInProgress:
			anyActive = true
			allDone = false
		case StatusNotStarted:
			allDone = false
		}
	}
	if allDone {
		return StatusDone
	}
	if anyActive {
		return StatusInProgress
	}
	return StatusNotStarted
}

func canGoForward(from, to StepStatus) bool {
	if from == to { return true }
	switch from {
	case StatusNotStarted:
		return to == StatusInProgress || to == StatusDone
	case StatusInProgress:
		return to == StatusDone
	case StatusDone:
		return false
	default:
		return false
	}
}
```

**Determinism:**

* We evaluate guards against a **snapshot** captured before any mutation.
* Parent transitions & substep transitions are both considered for the incoming event; then aggregation may update the parent once at the end, keeping things predictable.

---

## Guarding by substep

Guards can now reference either a step or a specific substep:

```json
"require": [
  { "step": "Billing", "status": "done" },
  { "step": "Customs", "substep": "Docs Check", "status": "done" }
]
```

---

## Patterns you might like

* **Parent-only via aggregation**: Omit the parent’s own `machine.onEvent` and set `deriveParentFromSubsteps=true` to make the parent purely computed.
* **Hybrid**: Keep parent transitions (e.g., to open/close the whole stage) *and* aggregate; the final aggregation will reconcile after substeps move.
* **Auto-start first substep** on entering `in_progress` at parent: add a tiny `onEnter` hook if you want (can be implemented as a synthetic transition fired when parent becomes `in_progress`).

---

If you share a concrete example (events + 2–3 steps with 1–3 substeps each), I’ll add a **table-driven test** that locks the exact behavior and a small **validator** that lints configs on write (unknown step/substep names, unreachable transitions, illegal backward moves when `forwardOnly` is true).
