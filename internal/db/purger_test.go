package db

import (
	"testing"
	"time"
)

type mockPurgeable struct {
	purgedDates map[string]bool
	callCount   int
}

func (m *mockPurgeable) PurgeOldDays(keepDates map[string]bool) {
	m.purgedDates = keepDates
	m.callCount++
}

func TestDayContainerPurger_BuildKeepDates(t *testing.T) {
	p := NewDayContainerPurger(48)
	dates := p.buildKeepDates()

	today := time.Now().Format("20060102")
	yesterday := time.Now().Add(-24 * time.Hour).Format("20060102")

	if !dates[today] {
		t.Fatal("today should be in keepDates")
	}
	if !dates[yesterday] {
		t.Fatal("yesterday should be in keepDates")
	}
}

func TestDayContainerPurger_BuildKeepDates_72Hours(t *testing.T) {
	p := NewDayContainerPurger(72)
	dates := p.buildKeepDates()

	today := time.Now().Format("20060102")
	twoDaysAgo := time.Now().Add(-48 * time.Hour).Format("20060102")

	if !dates[today] {
		t.Fatal("today should be in keepDates")
	}
	if !dates[twoDaysAgo] {
		t.Fatal("2 days ago should be in keepDates for 72h window")
	}

	// Should have at least 3 dates
	if len(dates) < 2 {
		t.Fatalf("expected at least 2 dates, got %d", len(dates))
	}
}

func TestDayContainerPurger_Purge(t *testing.T) {
	m1 := &mockPurgeable{}
	m2 := &mockPurgeable{}

	p := NewDayContainerPurger(48, m1, m2)
	p.purge()

	if m1.callCount != 1 {
		t.Fatalf("expected m1 called 1 time, got %d", m1.callCount)
	}
	if m2.callCount != 1 {
		t.Fatalf("expected m2 called 1 time, got %d", m2.callCount)
	}

	today := time.Now().Format("20060102")
	if !m1.purgedDates[today] {
		t.Fatal("today should be in keepDates passed to m1")
	}
}

func TestDayContainerPurger_DefaultKeepHours(t *testing.T) {
	p := NewDayContainerPurger(0)
	if p.keepHours != 48 {
		t.Fatalf("expected default 48, got %d", p.keepHours)
	}
}
