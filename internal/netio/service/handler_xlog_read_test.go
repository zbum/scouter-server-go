package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/counter"
	"github.com/zbum/scouter-server-go/internal/db/profile"
	"github.com/zbum/scouter-server-go/internal/db/xlog"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// buildRequest serializes a MapPack into a DataInputX that handlers can read.
func buildRequest(param *pack.MapPack) *protocol.DataInputX {
	o := protocol.NewDataOutputX()
	pack.WritePack(o, param)
	return protocol.NewDataInputX(o.ToByteArray())
}

// TestXLogReadByTxid writes an XLog, then reads it back via the XLOG_READ_BY_TXID handler.
func TestXLogReadByTxid(t *testing.T) {
	baseDir := t.TempDir()

	// Write test XLog data using the writer
	writer := xlog.NewXLogWR(baseDir)
	ctx, cancel := context.WithCancel(context.Background())
	writer.Start(ctx)

	now := time.Date(2026, 2, 7, 14, 0, 0, 0, time.UTC)
	date := now.Format("20060102")

	// Create a serialized XLogPack
	xp := &pack.XLogPack{
		EndTime: now.UnixMilli(),
		ObjHash: 100,
		Service: 200,
		Txid:    77001,
		Gxid:    88001,
		Elapsed: 500,
	}
	xpOut := protocol.NewDataOutputX()
	pack.WritePack(xpOut, xp)
	xpBytes := xpOut.ToByteArray()

	writer.Add(&xlog.XLogEntry{
		Time:    now.UnixMilli(),
		Txid:    77001,
		Gxid:    88001,
		Elapsed: 500,
		Data:    xpBytes,
	})

	time.Sleep(200 * time.Millisecond)
	cancel()
	writer.Close()

	// Set up reader and handler
	reader := xlog.NewXLogRD(baseDir)
	defer reader.Close()
	profileRD := profile.NewProfileRD(baseDir)
	defer profileRD.Close()

	registry := NewRegistry()
	RegisterXLogReadHandlers(registry, reader, profileRD, nil, xlog.NewXLogWR(baseDir))

	// Build request
	param := &pack.MapPack{}
	param.PutStr("date", date)
	param.PutLong("txid", 77001)

	din := buildRequest(param)
	dout := protocol.NewDataOutputX()

	handler := registry.Get(protocol.XLOG_READ_BY_TXID)
	if handler == nil {
		t.Fatal("XLOG_READ_BY_TXID handler not registered")
	}
	handler(din, dout, true)

	// Parse response
	result := dout.ToByteArray()
	if len(result) == 0 {
		t.Fatal("expected non-empty response from XLOG_READ_BY_TXID")
	}

	// First byte should be FLAG_HAS_NEXT
	if result[0] != protocol.FLAG_HAS_NEXT {
		t.Fatalf("expected FLAG_HAS_NEXT (0x%02x), got 0x%02x", protocol.FLAG_HAS_NEXT, result[0])
	}

	// The rest should be the XLogPack bytes
	respDin := protocol.NewDataInputX(result[1:])
	respPack, err := pack.ReadPack(respDin)
	if err != nil {
		t.Fatalf("failed to read response pack: %v", err)
	}
	respXLog, ok := respPack.(*pack.XLogPack)
	if !ok {
		t.Fatal("expected XLogPack in response")
	}
	if respXLog.Txid != 77001 {
		t.Errorf("expected txid 77001, got %d", respXLog.Txid)
	}
	if respXLog.ObjHash != 100 {
		t.Errorf("expected objHash 100, got %d", respXLog.ObjHash)
	}
	if respXLog.Elapsed != 500 {
		t.Errorf("expected elapsed 500, got %d", respXLog.Elapsed)
	}
}

// TestXLogReadByTxidNotFound tests that a missing txid produces an empty response.
func TestXLogReadByTxidNotFound(t *testing.T) {
	baseDir := t.TempDir()

	reader := xlog.NewXLogRD(baseDir)
	defer reader.Close()
	profileRD := profile.NewProfileRD(baseDir)
	defer profileRD.Close()

	registry := NewRegistry()
	RegisterXLogReadHandlers(registry, reader, profileRD, nil, xlog.NewXLogWR(baseDir))

	param := &pack.MapPack{}
	param.PutStr("date", "20260207")
	param.PutLong("txid", 99999)

	din := buildRequest(param)
	dout := protocol.NewDataOutputX()

	handler := registry.Get(protocol.XLOG_READ_BY_TXID)
	handler(din, dout, true)

	if len(dout.ToByteArray()) != 0 {
		t.Error("expected empty response for non-existent txid")
	}
}

// TestXLogReadByGxid writes two XLogs with the same gxid, reads them both back.
func TestXLogReadByGxid(t *testing.T) {
	baseDir := t.TempDir()

	writer := xlog.NewXLogWR(baseDir)
	ctx, cancel := context.WithCancel(context.Background())
	writer.Start(ctx)

	now := time.Date(2026, 2, 7, 14, 0, 0, 0, time.UTC)
	date := now.Format("20060102")
	gxid := int64(88002)

	for i := 0; i < 2; i++ {
		xp := &pack.XLogPack{
			EndTime: now.UnixMilli() + int64(i*1000),
			ObjHash: int32(100 + i),
			Txid:    int64(77100 + i),
			Gxid:    gxid,
			Elapsed: int32(100 + i*50),
		}
		xpOut := protocol.NewDataOutputX()
		pack.WritePack(xpOut, xp)

		writer.Add(&xlog.XLogEntry{
			Time:    xp.EndTime,
			Txid:    xp.Txid,
			Gxid:    gxid,
			Elapsed: xp.Elapsed,
			Data:    xpOut.ToByteArray(),
		})
	}

	time.Sleep(200 * time.Millisecond)
	cancel()
	writer.Close()

	reader := xlog.NewXLogRD(baseDir)
	defer reader.Close()
	profileRD := profile.NewProfileRD(baseDir)
	defer profileRD.Close()

	registry := NewRegistry()
	RegisterXLogReadHandlers(registry, reader, profileRD, nil, xlog.NewXLogWR(baseDir))

	param := &pack.MapPack{}
	param.PutStr("date", date)
	param.PutLong("gxid", gxid)

	din := buildRequest(param)
	dout := protocol.NewDataOutputX()

	handler := registry.Get(protocol.XLOG_READ_BY_GXID)
	if handler == nil {
		t.Fatal("XLOG_READ_BY_GXID handler not registered")
	}
	handler(din, dout, true)

	result := dout.ToByteArray()
	if len(result) == 0 {
		t.Fatal("expected non-empty response from XLOG_READ_BY_GXID")
	}

	// Count FLAG_HAS_NEXT occurrences by parsing the response stream
	respDin := protocol.NewDataInputX(result)
	count := 0
	for respDin.Available() > 0 {
		flag, err := respDin.ReadByte()
		if err != nil {
			break
		}
		if flag != protocol.FLAG_HAS_NEXT {
			t.Fatalf("expected FLAG_HAS_NEXT, got 0x%02x", flag)
		}
		_, err = pack.ReadPack(respDin)
		if err != nil {
			t.Fatalf("failed to read pack at index %d: %v", count, err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 XLog entries for gxid, got %d", count)
	}
}

// TestTranxProfile writes a profile, reads it back via the TRANX_PROFILE handler.
func TestTranxProfile(t *testing.T) {
	baseDir := t.TempDir()

	// Write profile using the writer
	profileWR := profile.NewProfileWR(baseDir, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	profileWR.Start(ctx)

	now := time.Date(2026, 2, 7, 14, 0, 0, 0, time.UTC)
	date := now.Format("20060102")
	txid := int64(55001)

	profileWR.Add(&profile.ProfileEntry{
		TimeMs: now.UnixMilli(),
		Txid:   txid,
		Data:   []byte("step1:method_call:100ms"),
	})
	profileWR.Add(&profile.ProfileEntry{
		TimeMs: now.UnixMilli(),
		Txid:   txid,
		Data:   []byte("step2:sql_query:50ms"),
	})

	time.Sleep(200 * time.Millisecond)
	cancel()
	profileWR.Close()

	// Re-open profileWR so it shares the same MemHashBlock state
	profileWR2 := profile.NewProfileWR(baseDir, 1000)

	xlogRD := xlog.NewXLogRD(baseDir)
	defer xlogRD.Close()

	registry := NewRegistry()
	RegisterXLogReadHandlers(registry, xlogRD, nil, profileWR2, xlog.NewXLogWR(baseDir))

	param := &pack.MapPack{}
	param.PutStr("date", date)
	param.PutLong("txid", txid)

	din := buildRequest(param)
	dout := protocol.NewDataOutputX()

	handler := registry.Get(protocol.TRANX_PROFILE)
	if handler == nil {
		t.Fatal("TRANX_PROFILE handler not registered")
	}
	handler(din, dout, true)

	result := dout.ToByteArray()
	if len(result) == 0 {
		t.Fatal("expected non-empty response from TRANX_PROFILE")
	}

	// Parse: FLAG_HAS_NEXT + XLogProfilePack (all blocks concatenated)
	respDin := protocol.NewDataInputX(result)
	flag, err := respDin.ReadByte()
	if err != nil {
		t.Fatalf("failed to read flag: %v", err)
	}
	if flag != protocol.FLAG_HAS_NEXT {
		t.Fatalf("expected FLAG_HAS_NEXT, got 0x%02x", flag)
	}
	profilePack, err := pack.ReadPack(respDin)
	if err != nil {
		t.Fatalf("failed to read XLogProfilePack: %v", err)
	}
	pp, ok := profilePack.(*pack.XLogProfilePack)
	if !ok {
		t.Fatalf("expected XLogProfilePack, got %T", profilePack)
	}
	// Profile contains concatenated blocks
	profileStr := string(pp.Profile)
	if !strings.Contains(profileStr, "step1:method_call:100ms") {
		t.Error("missing profile block 'step1:method_call:100ms'")
	}
	if !strings.Contains(profileStr, "step2:sql_query:50ms") {
		t.Error("missing profile block 'step2:sql_query:50ms'")
	}
}

// TestTranxProfileNotFound tests reading a profile for non-existent txid.
func TestTranxProfileNotFound(t *testing.T) {
	baseDir := t.TempDir()

	xlogRD := xlog.NewXLogRD(baseDir)
	defer xlogRD.Close()
	profileWR := profile.NewProfileWR(baseDir, 1000)
	defer profileWR.Close()

	registry := NewRegistry()
	RegisterXLogReadHandlers(registry, xlogRD, nil, profileWR, xlog.NewXLogWR(baseDir))

	param := &pack.MapPack{}
	param.PutStr("date", "20260207")
	param.PutLong("txid", 99999)

	din := buildRequest(param)
	dout := protocol.NewDataOutputX()

	handler := registry.Get(protocol.TRANX_PROFILE)
	handler(din, dout, true)

	if len(dout.ToByteArray()) != 0 {
		t.Error("expected empty response for non-existent profile")
	}
}

// TestCounterPastTime writes realtime counter data, reads it back via COUNTER_PAST_TIME handler.
func TestCounterPastTime(t *testing.T) {
	baseDir := t.TempDir()

	// Write counter data using the writer
	counterWR := counter.NewCounterWR(baseDir)
	ctx, cancel := context.WithCancel(context.Background())
	counterWR.Start(ctx)

	// Use current local time to match the writer's timezone behavior.
	// The writer converts TimeMs -> local time -> timeSec using local timezone.
	now := time.Now()
	// Round to start of the current second
	now = now.Truncate(time.Second)
	date := now.Format("20060102")
	objHash := int32(1)

	// Compute the seconds-of-day in local time (same as the writer does)
	baseSec := int32(now.Hour()*3600 + now.Minute()*60 + now.Second())

	// Write counters at baseSec..baseSec+5
	for sec := 0; sec < 6; sec++ {
		ts := now.Add(time.Duration(sec) * time.Second)
		counters := map[string]value.Value{
			"TPS": value.NewDecimalValue(int64(100 + sec)),
		}
		counterWR.AddRealtime(&counter.RealtimeEntry{
			TimeMs:   ts.UnixMilli(),
			ObjHash:  objHash,
			Counters: counters,
		})
	}

	time.Sleep(300 * time.Millisecond)
	cancel()
	counterWR.Close()

	// Read back
	counterRD := counter.NewCounterRD(baseDir)
	defer counterRD.Close()
	objectCache := cache.NewObjectCache()

	registry := NewRegistry()
	RegisterCounterReadHandlers(registry, counterRD, objectCache, 30*time.Second)

	param := &pack.MapPack{}
	param.PutStr("date", date)
	param.Put("objHash", value.NewDecimalValue(int64(objHash)))
	param.PutStr("counter", "TPS")
	param.Put("stime", value.NewDecimalValue(int64(baseSec)))
	param.Put("etime", value.NewDecimalValue(int64(baseSec+5)))

	din := buildRequest(param)
	dout := protocol.NewDataOutputX()

	handler := registry.Get(protocol.COUNTER_PAST_TIME)
	if handler == nil {
		t.Fatal("COUNTER_PAST_TIME handler not registered")
	}
	handler(din, dout, true)

	result := dout.ToByteArray()
	if len(result) == 0 {
		t.Fatal("expected non-empty response from COUNTER_PAST_TIME")
	}

	// Parse: FLAG_HAS_NEXT + MapPack with "time" and "value" lists
	respDin := protocol.NewDataInputX(result)
	flag, err := respDin.ReadByte()
	if err != nil || flag != protocol.FLAG_HAS_NEXT {
		t.Fatalf("expected FLAG_HAS_NEXT, got 0x%02x, err=%v", flag, err)
	}

	respPack, err := pack.ReadPack(respDin)
	if err != nil {
		t.Fatalf("failed to read response pack: %v", err)
	}
	mp, ok := respPack.(*pack.MapPack)
	if !ok {
		t.Fatal("expected MapPack in response")
	}

	timeVal := mp.Get("time")
	if timeVal == nil {
		t.Fatal("missing 'time' in response")
	}
	timeList, ok := timeVal.(*value.ListValue)
	if !ok {
		t.Fatal("expected ListValue for 'time'")
	}

	valueVal := mp.Get("value")
	if valueVal == nil {
		t.Fatal("missing 'value' in response")
	}
	valueList, ok := valueVal.(*value.ListValue)
	if !ok {
		t.Fatal("expected ListValue for 'value'")
	}

	if len(timeList.Value) != 6 {
		t.Fatalf("expected 6 time entries, got %d", len(timeList.Value))
	}
	if len(valueList.Value) != 6 {
		t.Fatalf("expected 6 value entries, got %d", len(valueList.Value))
	}

	// Verify first entry value
	firstVal, ok := valueList.Value[0].(*value.DecimalValue)
	if !ok {
		t.Fatal("expected DecimalValue in value list")
	}
	if firstVal.Value != 100 {
		t.Errorf("expected first TPS=100, got %d", firstVal.Value)
	}
}

// TestCounterPastDate writes daily counter data, reads it back via COUNTER_PAST_DATE handler.
func TestCounterPastDate(t *testing.T) {
	baseDir := t.TempDir()

	// Write daily counter data using the writer
	counterWR := counter.NewCounterWR(baseDir)
	ctx, cancel := context.WithCancel(context.Background())
	counterWR.Start(ctx)

	date := "20260207"
	objHash := int32(1)

	// Write buckets 0 (00:00) and 144 (12:00)
	counterWR.AddDaily(&counter.DailyEntry{
		Date:        date,
		ObjHash:     objHash,
		CounterName: "TPS",
		Bucket:      0,
		Value:       42.5,
	})
	counterWR.AddDaily(&counter.DailyEntry{
		Date:        date,
		ObjHash:     objHash,
		CounterName: "TPS",
		Bucket:      144,
		Value:       100.0,
	})

	time.Sleep(300 * time.Millisecond)
	cancel()
	counterWR.Close()

	// Read back
	counterRD := counter.NewCounterRD(baseDir)
	defer counterRD.Close()
	objectCache := cache.NewObjectCache()

	registry := NewRegistry()
	RegisterCounterReadHandlers(registry, counterRD, objectCache, 30*time.Second)

	param := &pack.MapPack{}
	param.PutStr("date", date)
	param.Put("objHash", value.NewDecimalValue(int64(objHash)))
	param.PutStr("counter", "TPS")

	din := buildRequest(param)
	dout := protocol.NewDataOutputX()

	handler := registry.Get(protocol.COUNTER_PAST_DATE)
	if handler == nil {
		t.Fatal("COUNTER_PAST_DATE handler not registered")
	}
	handler(din, dout, true)

	result := dout.ToByteArray()
	if len(result) == 0 {
		t.Fatal("expected non-empty response from COUNTER_PAST_DATE")
	}

	// Parse: FLAG_HAS_NEXT + MapPack with "value" as FloatArray
	respDin := protocol.NewDataInputX(result)
	flag, err := respDin.ReadByte()
	if err != nil || flag != protocol.FLAG_HAS_NEXT {
		t.Fatalf("expected FLAG_HAS_NEXT, got 0x%02x, err=%v", flag, err)
	}

	respPack, err := pack.ReadPack(respDin)
	if err != nil {
		t.Fatalf("failed to read response pack: %v", err)
	}
	mp, ok := respPack.(*pack.MapPack)
	if !ok {
		t.Fatal("expected MapPack in response")
	}

	valField := mp.Get("value")
	if valField == nil {
		t.Fatal("missing 'value' in response")
	}

	floatArr, ok := valField.(*value.FloatArray)
	if !ok {
		t.Fatalf("expected FloatArray for 'value', got %T", valField)
	}

	if len(floatArr.Value) != 288 {
		t.Fatalf("expected 288 buckets, got %d", len(floatArr.Value))
	}

	// Check bucket 0
	if floatArr.Value[0] != float32(42.5) {
		t.Errorf("expected bucket[0]=42.5, got %f", floatArr.Value[0])
	}

	// Check bucket 144
	if floatArr.Value[144] != float32(100.0) {
		t.Errorf("expected bucket[144]=100.0, got %f", floatArr.Value[144])
	}

	// Check unwritten bucket (should be 0 since NaN is converted to 0)
	if floatArr.Value[1] != 0 {
		t.Errorf("expected bucket[1]=0 (unwritten), got %f", floatArr.Value[1])
	}
}

// TestCounterPastDateNotFound tests reading daily counter for non-existent data.
func TestCounterPastDateNotFound(t *testing.T) {
	baseDir := t.TempDir()

	counterRD := counter.NewCounterRD(baseDir)
	defer counterRD.Close()
	objectCache := cache.NewObjectCache()

	registry := NewRegistry()
	RegisterCounterReadHandlers(registry, counterRD, objectCache, 30*time.Second)

	param := &pack.MapPack{}
	param.PutStr("date", "20991231")
	param.Put("objHash", value.NewDecimalValue(1))
	param.PutStr("counter", "TPS")

	din := buildRequest(param)
	dout := protocol.NewDataOutputX()

	handler := registry.Get(protocol.COUNTER_PAST_DATE)
	handler(din, dout, true)

	if len(dout.ToByteArray()) != 0 {
		t.Error("expected empty response for non-existent daily counter")
	}
}

// TestTranxLoadTimeGroup writes multiple XLogs, reads by time range with objHash filter.
func TestTranxLoadTimeGroup(t *testing.T) {
	baseDir := t.TempDir()

	writer := xlog.NewXLogWR(baseDir)
	ctx, cancel := context.WithCancel(context.Background())
	writer.Start(ctx)

	now := time.Date(2026, 2, 7, 14, 0, 0, 0, time.UTC)
	date := now.Format("20060102")

	// Write 3 XLogs: 2 for objHash=100, 1 for objHash=200
	for i := 0; i < 3; i++ {
		objHash := int32(100)
		if i == 2 {
			objHash = 200
		}
		xp := &pack.XLogPack{
			EndTime: now.UnixMilli() + int64(i*1000),
			ObjHash: objHash,
			Service: int32(300 + i),
			Txid:    int64(66000 + i),
			Elapsed: int32(100 + i*50),
		}
		xpOut := protocol.NewDataOutputX()
		pack.WritePack(xpOut, xp)

		writer.Add(&xlog.XLogEntry{
			Time:    xp.EndTime,
			Txid:    xp.Txid,
			Gxid:    0,
			Elapsed: xp.Elapsed,
			Data:    xpOut.ToByteArray(),
		})
	}

	time.Sleep(200 * time.Millisecond)
	cancel()
	writer.Close()

	// Set up handler
	xlogRD := xlog.NewXLogRD(baseDir)
	defer xlogRD.Close()

	registry := NewRegistry()
	RegisterXLogReadHandlers(registry, xlogRD, nil, nil, xlog.NewXLogWR(baseDir))

	// Test without filter - should get all 3 + 1 metadata pack = 4 HAS_NEXT
	param := &pack.MapPack{}
	param.PutStr("date", date)
	param.PutLong("stime", now.UnixMilli()-1000)
	param.PutLong("etime", now.UnixMilli()+5000)

	din := buildRequest(param)
	dout := protocol.NewDataOutputX()

	handler := registry.Get(protocol.TRANX_LOAD_TIME_GROUP)
	if handler == nil {
		t.Fatal("TRANX_LOAD_TIME_GROUP handler not registered")
	}
	handler(din, dout, true)

	result := dout.ToByteArray()
	if len(result) == 0 {
		t.Fatal("expected non-empty response")
	}

	// Count the HAS_NEXT flags: 1 metadata + 3 xlogs = 4
	respDin := protocol.NewDataInputX(result)
	count := 0
	for respDin.Available() > 0 {
		flag, err := respDin.ReadByte()
		if err != nil {
			break
		}
		if flag != protocol.FLAG_HAS_NEXT {
			t.Fatalf("expected FLAG_HAS_NEXT, got 0x%02x", flag)
		}
		_, err = pack.ReadPack(respDin)
		if err != nil {
			t.Fatalf("failed to read pack at index %d: %v", count, err)
		}
		count++
	}

	// 3 XLogPacks (no metadata)
	if count != 3 {
		t.Errorf("expected 3 xlogs, got %d", count)
	}

	// Test with objHash filter - should get only objHash=100 (2 entries) + 1 metadata
	paramFiltered := &pack.MapPack{}
	paramFiltered.PutStr("date", date)
	paramFiltered.PutLong("stime", now.UnixMilli()-1000)
	paramFiltered.PutLong("etime", now.UnixMilli()+5000)
	filterList := value.NewListValue()
	filterList.Value = append(filterList.Value, value.NewDecimalValue(100))
	paramFiltered.Put("objHash", filterList)

	din2 := buildRequest(paramFiltered)
	dout2 := protocol.NewDataOutputX()
	handler(din2, dout2, true)

	result2 := dout2.ToByteArray()
	respDin2 := protocol.NewDataInputX(result2)
	count2 := 0
	for respDin2.Available() > 0 {
		flag, err := respDin2.ReadByte()
		if err != nil {
			break
		}
		if flag != protocol.FLAG_HAS_NEXT {
			break
		}
		_, err = pack.ReadPack(respDin2)
		if err != nil {
			break
		}
		count2++
	}

	// 2 filtered xlogs (no metadata)
	if count2 != 2 {
		t.Errorf("expected 2 filtered xlogs, got %d", count2)
	}

	// Verify V2 is also registered with the same handler
	handlerV2 := registry.Get(protocol.TRANX_LOAD_TIME_GROUP_V2)
	if handlerV2 == nil {
		t.Fatal("TRANX_LOAD_TIME_GROUP_V2 handler not registered")
	}
}

// TestCounterPastTimeAll tests reading realtime counter for all live objects of a type.
func TestCounterPastTimeAll(t *testing.T) {
	baseDir := t.TempDir()

	// Write counter data for two objects
	counterWR := counter.NewCounterWR(baseDir)
	ctx, cancel := context.WithCancel(context.Background())
	counterWR.Start(ctx)

	// Use current local time to match writer's timezone behavior
	now := time.Now().Truncate(time.Second)
	date := now.Format("20060102")
	timeSec := int32(now.Hour()*3600 + now.Minute()*60 + now.Second())

	for _, objHash := range []int32{1, 2} {
		counters := map[string]value.Value{
			"TPS": value.NewDecimalValue(int64(50 * objHash)),
		}
		counterWR.AddRealtime(&counter.RealtimeEntry{
			TimeMs:   now.UnixMilli(),
			ObjHash:  objHash,
			Counters: counters,
		})
	}

	time.Sleep(300 * time.Millisecond)
	cancel()
	counterWR.Close()

	// Set up object cache with two live objects of type "tomcat"
	objectCache := cache.NewObjectCache()
	objectCache.Put(1, &pack.ObjectPack{ObjType: "tomcat", ObjHash: 1, ObjName: "/host/obj1", Alive: true})
	objectCache.Put(2, &pack.ObjectPack{ObjType: "tomcat", ObjHash: 2, ObjName: "/host/obj2", Alive: true})

	counterRD := counter.NewCounterRD(baseDir)
	defer counterRD.Close()

	registry := NewRegistry()
	RegisterCounterReadHandlers(registry, counterRD, objectCache, 30*time.Second)

	param := &pack.MapPack{}
	param.PutStr("date", date)
	param.PutStr("counter", "TPS")
	param.PutStr("objType", "tomcat")
	param.Put("stime", value.NewDecimalValue(int64(timeSec)))
	param.Put("etime", value.NewDecimalValue(int64(timeSec)))

	din := buildRequest(param)
	dout := protocol.NewDataOutputX()

	handler := registry.Get(protocol.COUNTER_PAST_TIME_ALL)
	if handler == nil {
		t.Fatal("COUNTER_PAST_TIME_ALL handler not registered")
	}
	handler(din, dout, true)

	result := dout.ToByteArray()
	if len(result) == 0 {
		t.Fatal("expected non-empty response from COUNTER_PAST_TIME_ALL")
	}

	// Parse: each object gets FLAG_HAS_NEXT + MapPack
	respDin := protocol.NewDataInputX(result)
	count := 0
	for respDin.Available() > 0 {
		flag, err := respDin.ReadByte()
		if err != nil {
			break
		}
		if flag != protocol.FLAG_HAS_NEXT {
			break
		}
		_, err = pack.ReadPack(respDin)
		if err != nil {
			break
		}
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 result packs (one per object), got %d", count)
	}
}

// TestCounterPastDateAll tests reading daily counter for all live objects of a type.
func TestCounterPastDateAll(t *testing.T) {
	baseDir := t.TempDir()

	counterWR := counter.NewCounterWR(baseDir)
	ctx, cancel := context.WithCancel(context.Background())
	counterWR.Start(ctx)

	date := "20260207"

	counterWR.AddDaily(&counter.DailyEntry{
		Date: date, ObjHash: 1, CounterName: "TPS", Bucket: 0, Value: 10.0,
	})
	counterWR.AddDaily(&counter.DailyEntry{
		Date: date, ObjHash: 2, CounterName: "TPS", Bucket: 0, Value: 20.0,
	})

	time.Sleep(300 * time.Millisecond)
	cancel()
	counterWR.Close()

	objectCache := cache.NewObjectCache()
	objectCache.Put(1, &pack.ObjectPack{ObjType: "tomcat", ObjHash: 1, ObjName: "/host/obj1", Alive: true})
	objectCache.Put(2, &pack.ObjectPack{ObjType: "tomcat", ObjHash: 2, ObjName: "/host/obj2", Alive: true})

	counterRD := counter.NewCounterRD(baseDir)
	defer counterRD.Close()

	registry := NewRegistry()
	RegisterCounterReadHandlers(registry, counterRD, objectCache, 30*time.Second)

	param := &pack.MapPack{}
	param.PutStr("date", date)
	param.PutStr("counter", "TPS")
	param.PutStr("objType", "tomcat")

	din := buildRequest(param)
	dout := protocol.NewDataOutputX()

	handler := registry.Get(protocol.COUNTER_PAST_DATE_ALL)
	if handler == nil {
		t.Fatal("COUNTER_PAST_DATE_ALL handler not registered")
	}
	handler(din, dout, true)

	result := dout.ToByteArray()
	if len(result) == 0 {
		t.Fatal("expected non-empty response from COUNTER_PAST_DATE_ALL")
	}

	// Parse result packs
	respDin := protocol.NewDataInputX(result)
	count := 0
	for respDin.Available() > 0 {
		flag, err := respDin.ReadByte()
		if err != nil {
			break
		}
		if flag != protocol.FLAG_HAS_NEXT {
			break
		}
		respPack, err := pack.ReadPack(respDin)
		if err != nil {
			break
		}
		mp, ok := respPack.(*pack.MapPack)
		if !ok {
			t.Fatal("expected MapPack")
		}

		// Verify each result has objHash and value
		objHashVal := mp.GetLong("objHash")
		if objHashVal != 1 && objHashVal != 2 {
			t.Errorf("unexpected objHash: %d", objHashVal)
		}

		valField := mp.Get("value")
		if valField == nil {
			t.Fatal("missing 'value' in response")
		}
		floatArr, ok := valField.(*value.FloatArray)
		if !ok {
			t.Fatalf("expected FloatArray, got %T", valField)
		}
		if len(floatArr.Value) != 288 {
			t.Fatalf("expected 288 buckets, got %d", len(floatArr.Value))
		}

		count++
	}

	if count != 2 {
		t.Errorf("expected 2 result packs (one per object), got %d", count)
	}
}
