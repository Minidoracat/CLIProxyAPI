package cliproxy

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

type fakeModelPriceStore struct {
	loaded  map[string]config.ModelPrice
	loadErr error
	saved   map[string]config.ModelPrice
	saveErr error
}

func (f *fakeModelPriceStore) LoadModelPrices(context.Context) (map[string]config.ModelPrice, error) {
	return f.loaded, f.loadErr
}

func (f *fakeModelPriceStore) SaveModelPrices(_ context.Context, prices map[string]config.ModelPrice) error {
	f.saved = prices
	return f.saveErr
}

func TestReconcileModelPricesDatabaseWins(t *testing.T) {
	configured := map[string]config.ModelPrice{"gpt-5.5": {Prompt: 1}}
	stored := map[string]config.ModelPrice{"gpt-5.6-sol": {Prompt: 5, Completion: 30, Cache: 0.5}}
	store := &fakeModelPriceStore{loaded: stored}

	got, seeded, err := reconcileModelPrices(context.Background(), store, configured)
	if err != nil {
		t.Fatalf("reconcileModelPrices: %v", err)
	}
	if seeded {
		t.Fatal("seeded = true, want false")
	}
	if !reflect.DeepEqual(got, stored) {
		t.Fatalf("prices = %#v, want %#v", got, stored)
	}
	if store.saved != nil {
		t.Fatalf("unexpected save: %#v", store.saved)
	}
}

func TestReconcileModelPricesSeedsEmptyDatabase(t *testing.T) {
	configured := map[string]config.ModelPrice{"gpt-5.6-sol": {Prompt: 5, Completion: 30, Cache: 0.5}}
	store := &fakeModelPriceStore{loaded: map[string]config.ModelPrice{}}

	got, seeded, err := reconcileModelPrices(context.Background(), store, configured)
	if err != nil {
		t.Fatalf("reconcileModelPrices: %v", err)
	}
	if !seeded {
		t.Fatal("seeded = false, want true")
	}
	if !reflect.DeepEqual(got, configured) || !reflect.DeepEqual(store.saved, configured) {
		t.Fatalf("got=%#v saved=%#v want=%#v", got, store.saved, configured)
	}
}

func TestReconcileModelPricesPreservesConfigOnLoadError(t *testing.T) {
	configured := map[string]config.ModelPrice{"gpt-5.6-luna": {Prompt: 1, Completion: 6, Cache: 0.1}}
	store := &fakeModelPriceStore{loadErr: errors.New("load failed")}

	got, seeded, err := reconcileModelPrices(context.Background(), store, configured)
	if err == nil {
		t.Fatal("error = nil, want load failure")
	}
	if seeded {
		t.Fatal("seeded = true, want false")
	}
	if !reflect.DeepEqual(got, configured) {
		t.Fatalf("prices = %#v, want %#v", got, configured)
	}
}
