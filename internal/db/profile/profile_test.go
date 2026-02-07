package profile

import (
	"context"
	"testing"
	"time"
)

func TestProfileData_WriteRead(t *testing.T) {
	dir := t.TempDir()

	data, err := NewProfileData(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	// Write single block
	block1 := []byte("step1:method_call:100ms")
	if err := data.Write(111, block1); err != nil {
		t.Fatal(err)
	}
	data.Flush()

	// Read back
	blocks, err := data.Read(111, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 {
		t.Fatalf("expected 1 block, got %d", len(blocks))
	}
	if string(blocks[0]) != string(block1) {
		t.Fatalf("expected %s, got %s", block1, blocks[0])
	}
}

func TestProfileData_MultipleBlocks(t *testing.T) {
	dir := t.TempDir()

	data, err := NewProfileData(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	// Write multiple blocks for same txid
	data.Write(222, []byte("block-A"))
	data.Write(222, []byte("block-B"))
	data.Write(222, []byte("block-C"))
	data.Flush()

	blocks, err := data.Read(222, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 3 {
		t.Fatalf("expected 3 blocks, got %d", len(blocks))
	}

	// With limit
	blocks, err = data.Read(222, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 2 {
		t.Fatalf("expected 2 blocks (limited), got %d", len(blocks))
	}
}

func TestProfileData_MultipleTxids(t *testing.T) {
	dir := t.TempDir()

	data, err := NewProfileData(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	data.Write(100, []byte("tx100-profile"))
	data.Write(200, []byte("tx200-profile"))
	data.Flush()

	b1, _ := data.Read(100, -1)
	b2, _ := data.Read(200, -1)

	if len(b1) != 1 || string(b1[0]) != "tx100-profile" {
		t.Fatal("tx100 profile mismatch")
	}
	if len(b2) != 1 || string(b2[0]) != "tx200-profile" {
		t.Fatal("tx200 profile mismatch")
	}
}

func TestProfileData_NonExistent(t *testing.T) {
	dir := t.TempDir()

	data, err := NewProfileData(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	blocks, err := data.Read(999, -1)
	if err != nil {
		t.Fatal(err)
	}
	if blocks != nil {
		t.Fatal("expected nil for non-existent txid")
	}
}

func TestProfileWR_Async(t *testing.T) {
	baseDir := t.TempDir()

	wr := NewProfileWR(baseDir)
	ctx, cancel := context.WithCancel(context.Background())
	wr.Start(ctx)

	now := time.Now()
	wr.Add(&ProfileEntry{
		TimeMs: now.UnixMilli(),
		Txid:   555,
		Data:   []byte("async-profile-data"),
	})

	time.Sleep(200 * time.Millisecond)
	cancel()
	wr.Close()

	// Verify
	rd := NewProfileRD(baseDir)
	defer rd.Close()

	date := now.Format("20060102")
	blocks, err := rd.GetProfile(date, 555, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 {
		t.Fatalf("expected 1 block, got %d", len(blocks))
	}
	if string(blocks[0]) != "async-profile-data" {
		t.Fatalf("expected async-profile-data, got %s", blocks[0])
	}
}

func TestProfileRD_NonExistentDate(t *testing.T) {
	baseDir := t.TempDir()
	rd := NewProfileRD(baseDir)
	defer rd.Close()

	blocks, err := rd.GetProfile("99991231", 1, -1)
	if err != nil {
		t.Fatal(err)
	}
	if blocks != nil {
		t.Fatal("expected nil for non-existent date")
	}
}
