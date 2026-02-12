package core

import (
	"log/slog"
	"sync"
	"time"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/text"
	"github.com/zbum/scouter-server-go/internal/util"
)

const sqlTableTextType = "sqltable"

// SqlTables extracts table names from SQL statements asynchronously.
type SqlTables struct {
	mu        sync.Mutex
	textCache *cache.TextCache
	textWR    *text.TextWR
	queue     chan *sqlTableEntry
	parsedSet map[parsedKey]struct{}
	lastDate  string
}

type sqlTableEntry struct {
	date    string
	sqlHash int32
	sqlText string
}

type parsedKey struct {
	date    string
	sqlHash int32
}

// NewSqlTables creates a new SQL table name extractor.
func NewSqlTables(textCache *cache.TextCache, textWR *text.TextWR) *SqlTables {
	st := &SqlTables{
		textCache: textCache,
		textWR:    textWR,
		queue:     make(chan *sqlTableEntry, 4096),
		parsedSet: make(map[parsedKey]struct{}),
		lastDate:  time.Now().Format("20060102"),
	}
	go st.run()
	return st
}

// Add queues a SQL statement for table name extraction.
// sqlHash is the hash of the SQL text, sqlText is the resolved SQL string.
func (st *SqlTables) Add(date string, sqlHash int32, sqlText string) {
	if sqlHash == 0 || sqlText == "" {
		return
	}
	select {
	case st.queue <- &sqlTableEntry{date: date, sqlHash: sqlHash, sqlText: sqlText}:
	default:
	}
}

func (st *SqlTables) run() {
	for entry := range st.queue {
		st.process(entry)
	}
}

func (st *SqlTables) process(entry *sqlTableEntry) {
	st.mu.Lock()
	// Reset parsed set on date change
	if entry.date != st.lastDate {
		st.parsedSet = make(map[parsedKey]struct{})
		st.lastDate = entry.date
	}

	key := parsedKey{date: entry.date, sqlHash: entry.sqlHash}
	if _, exists := st.parsedSet[key]; exists {
		st.mu.Unlock()
		return
	}
	st.parsedSet[key] = struct{}{}
	st.mu.Unlock()

	tableInfo := util.ParseTableInfo(entry.sqlText)
	if tableInfo == "" {
		if cfg := config.Get(); cfg != nil && cfg.LogSqlParsingFailEnabled() {
			slog.Debug("SQL table parsing failed", "sqlHash", entry.sqlHash, "sql", entry.sqlText)
		}
		return
	}

	// Store table info as text with the SQL hash as key
	if st.textCache != nil {
		st.textCache.Put(sqlTableTextType, entry.sqlHash, tableInfo)
	}
	if st.textWR != nil {
		st.textWR.Add(sqlTableTextType, entry.sqlHash, tableInfo)
	}
}
