package sequencer

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDoCapturesValueThroughClosure(t *testing.T) {
	seq := mustNewSequencer(t)

	type Order struct {
		ID string
	}

	var order Order
	err := seq.Do(context.Background(), "u-1", func(context.Context) error {
		order = Order{ID: "order-1"}
		return nil
	})
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if order.ID != "order-1" {
		t.Fatalf("got order id %q, want %q", order.ID, "order-1")
	}
}

func TestDifferentResultShapesSameKeyRunSequentially(t *testing.T) {
	seq := mustNewSequencer(t)

	type Order struct {
		ID string
	}
	type Balance struct {
		Available int64
	}

	firstGate := make(chan struct{})
	firstStarted := make(chan struct{}, 1)
	secondStarted := make(chan struct{}, 1)

	var (
		order   Order
		balance Balance
	)

	first, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		firstStarted <- struct{}{}
		order = Order{ID: "order-1"}
		<-firstGate
		return nil
	})
	if err != nil {
		t.Fatalf("submit first: %v", err)
	}
	second, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		secondStarted <- struct{}{}
		balance = Balance{Available: 100}
		return nil
	})
	if err != nil {
		t.Fatalf("submit second: %v", err)
	}

	assertReceiveStruct(t, firstStarted)
	assertNoReceiveStruct(t, secondStarted, 120*time.Millisecond)

	close(firstGate)
	assertReceiveStruct(t, secondStarted)

	waitAll(t, first, second)

	if order.ID != "order-1" {
		t.Fatalf("got order id %q, want %q", order.ID, "order-1")
	}
	if balance.Available != 100 {
		t.Fatalf("got available %d, want %d", balance.Available, 100)
	}
}

func TestDoReturnsTaskError(t *testing.T) {
	seq := mustNewSequencer(t)

	want := errors.New("boom")
	err := seq.Do(context.Background(), "u-1", func(context.Context) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}

