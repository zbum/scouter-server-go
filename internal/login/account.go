package login

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// Account represents a user account.
type Account struct {
	ID       string
	Password string
	Email    string
	Group    string
}

// ToBytes serializes the Account using 1-byte-length-prefixed fields
// matching the Java Account.toBytes() format.
func (a *Account) ToBytes() []byte {
	var buf bytes.Buffer
	writeField := func(s string) {
		b := []byte(s)
		if len(b) > 255 {
			b = b[:255]
		}
		buf.WriteByte(byte(len(b)))
		buf.Write(b)
	}
	writeField(a.ID)
	writeField(a.Password)
	writeField(a.Email)
	writeField(a.Group)
	return buf.Bytes()
}

// AccountFromBytes deserializes an Account from 1-byte-length-prefixed fields.
func AccountFromBytes(data []byte) *Account {
	offset := 0
	readField := func() string {
		if offset >= len(data) {
			return ""
		}
		length := int(data[offset])
		offset++
		if offset+length > len(data) {
			s := string(data[offset:])
			offset = len(data)
			return s
		}
		s := string(data[offset : offset+length])
		offset += length
		return s
	}
	return &Account{
		ID:       readField(),
		Password: readField(),
		Email:    readField(),
		Group:    readField(),
	}
}

// AccountManager manages user accounts and group policies,
// persisted as XML files in the conf directory.
type AccountManager struct {
	mu             sync.RWMutex
	confDir        string
	accountMap     map[string]*Account
	groupPolicyMap map[string]*value.MapValue

	accountModTime time.Time
	groupModTime   time.Time
}

// NewAccountManager creates an AccountManager that stores XML files in confDir.
// It initializes default XML files if they don't exist and loads them.
func NewAccountManager(confDir string) *AccountManager {
	am := &AccountManager{
		confDir:        confDir,
		accountMap:     make(map[string]*Account),
		groupPolicyMap: make(map[string]*value.MapValue),
	}
	am.ensureDefaults()
	am.loadAccounts()
	am.loadGroups()
	return am
}

func (am *AccountManager) accountFilePath() string {
	return filepath.Join(am.confDir, "account.xml")
}

func (am *AccountManager) groupFilePath() string {
	return filepath.Join(am.confDir, "account_group.xml")
}

// ensureDefaults writes the embedded default XML files if they don't exist on disk.
func (am *AccountManager) ensureDefaults() {
	if err := os.MkdirAll(am.confDir, 0755); err != nil {
		slog.Error("AccountManager: failed to create conf dir", "dir", am.confDir, "error", err)
		return
	}

	acctPath := am.accountFilePath()
	if _, err := os.Stat(acctPath); os.IsNotExist(err) {
		if err := os.WriteFile(acctPath, defaultAccountXML, 0644); err != nil {
			slog.Error("AccountManager: failed to write default account.xml", "error", err)
		} else {
			slog.Info("AccountManager: created default account.xml", "path", acctPath)
		}
	}

	grpPath := am.groupFilePath()
	if _, err := os.Stat(grpPath); os.IsNotExist(err) {
		if err := os.WriteFile(grpPath, defaultAccountGroupXML, 0644); err != nil {
			slog.Error("AccountManager: failed to write default account_group.xml", "error", err)
		} else {
			slog.Info("AccountManager: created default account_group.xml", "path", grpPath)
		}
	}
}

func (am *AccountManager) loadAccounts() {
	path := am.accountFilePath()
	info, err := os.Stat(path)
	if err != nil {
		slog.Warn("AccountManager: cannot stat account.xml", "error", err)
		return
	}
	accounts, err := parseAccountFile(path)
	if err != nil {
		slog.Error("AccountManager: failed to parse account.xml", "error", err)
		return
	}
	am.mu.Lock()
	am.accountMap = accounts
	am.accountModTime = info.ModTime()
	am.mu.Unlock()
	slog.Info("AccountManager: loaded accounts", "count", len(accounts))
}

func (am *AccountManager) loadGroups() {
	path := am.groupFilePath()
	info, err := os.Stat(path)
	if err != nil {
		slog.Warn("AccountManager: cannot stat account_group.xml", "error", err)
		return
	}
	groups, err := parseGroupFile(path)
	if err != nil {
		slog.Error("AccountManager: failed to parse account_group.xml", "error", err)
		return
	}
	am.mu.Lock()
	am.groupPolicyMap = groups
	am.groupModTime = info.ModTime()
	am.mu.Unlock()
	slog.Info("AccountManager: loaded groups", "count", len(groups))
}

// StartWatcher starts a goroutine that polls for XML file changes every 5 seconds.
func (am *AccountManager) StartWatcher(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				am.checkReload()
			}
		}
	}()
}

func (am *AccountManager) checkReload() {
	acctPath := am.accountFilePath()
	if info, err := os.Stat(acctPath); err == nil {
		am.mu.RLock()
		changed := info.ModTime().After(am.accountModTime)
		am.mu.RUnlock()
		if changed {
			am.loadAccounts()
		}
	}

	grpPath := am.groupFilePath()
	if info, err := os.Stat(grpPath); err == nil {
		am.mu.RLock()
		changed := info.ModTime().After(am.groupModTime)
		am.mu.RUnlock()
		if changed {
			am.loadGroups()
		}
	}
}

// AuthorizeAccount checks if the given id/pass combination is valid.
// The pass parameter is expected to be a SHA-256 hex string (client sends pre-hashed).
func (am *AccountManager) AuthorizeAccount(id, pass string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()
	acct, ok := am.accountMap[id]
	if !ok {
		return false
	}
	return acct.Password == pass
}

// AddAccount adds a new account and persists it to account.xml.
func (am *AccountManager) AddAccount(acct *Account) bool {
	am.mu.Lock()
	if _, exists := am.accountMap[acct.ID]; exists {
		am.mu.Unlock()
		return false
	}
	am.accountMap[acct.ID] = acct
	am.mu.Unlock()

	if err := addAccountToFile(am.accountFilePath(), acct); err != nil {
		slog.Error("AccountManager: failed to add account to file", "id", acct.ID, "error", err)
		return false
	}
	// Update mod time
	if info, err := os.Stat(am.accountFilePath()); err == nil {
		am.mu.Lock()
		am.accountModTime = info.ModTime()
		am.mu.Unlock()
	}
	return true
}

// EditAccount updates an existing account and persists to account.xml.
func (am *AccountManager) EditAccount(acct *Account) bool {
	am.mu.Lock()
	existing, ok := am.accountMap[acct.ID]
	if !ok {
		am.mu.Unlock()
		return false
	}
	// If password is empty, keep the existing password
	if acct.Password == "" {
		acct.Password = existing.Password
	}
	am.accountMap[acct.ID] = acct
	am.mu.Unlock()

	if err := editAccountInFile(am.accountFilePath(), acct); err != nil {
		slog.Error("AccountManager: failed to edit account in file", "id", acct.ID, "error", err)
		return false
	}
	if info, err := os.Stat(am.accountFilePath()); err == nil {
		am.mu.Lock()
		am.accountModTime = info.ModTime()
		am.mu.Unlock()
	}
	return true
}

// AvailableID returns true if the given ID is not already taken.
func (am *AccountManager) AvailableID(id string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()
	_, exists := am.accountMap[id]
	return !exists
}

// GetAccount returns the account for the given ID, or nil.
func (am *AccountManager) GetAccount(id string) *Account {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.accountMap[id]
}

// GetAccountList returns all accounts.
func (am *AccountManager) GetAccountList() []*Account {
	am.mu.RLock()
	defer am.mu.RUnlock()
	list := make([]*Account, 0, len(am.accountMap))
	for _, a := range am.accountMap {
		list = append(list, a)
	}
	return list
}

// GetGroupList returns all group names.
func (am *AccountManager) GetGroupList() []string {
	am.mu.RLock()
	defer am.mu.RUnlock()
	names := make([]string, 0, len(am.groupPolicyMap))
	for name := range am.groupPolicyMap {
		names = append(names, name)
	}
	return names
}

// GetGroupPolicy returns the policy MapValue for a group, or nil.
func (am *AccountManager) GetGroupPolicy(group string) *value.MapValue {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.groupPolicyMap[group]
}

// AllGroupPolicies returns a copy of all group policies.
func (am *AccountManager) AllGroupPolicies() map[string]*value.MapValue {
	am.mu.RLock()
	defer am.mu.RUnlock()
	m := make(map[string]*value.MapValue, len(am.groupPolicyMap))
	for k, v := range am.groupPolicyMap {
		m[k] = v
	}
	return m
}

// AddAccountGroup creates a new group with the given policy.
func (am *AccountManager) AddAccountGroup(name string, policy *value.MapValue) bool {
	am.mu.Lock()
	if _, exists := am.groupPolicyMap[name]; exists {
		am.mu.Unlock()
		return false
	}
	am.groupPolicyMap[name] = policy
	am.mu.Unlock()

	if err := addGroupToFile(am.groupFilePath(), name, policy); err != nil {
		slog.Error("AccountManager: failed to add group to file", "name", name, "error", err)
		return false
	}
	if info, err := os.Stat(am.groupFilePath()); err == nil {
		am.mu.Lock()
		am.groupModTime = info.ModTime()
		am.mu.Unlock()
	}
	return true
}

// EditGroupPolicy updates an existing group's policy.
func (am *AccountManager) EditGroupPolicy(name string, policy *value.MapValue) bool {
	am.mu.Lock()
	if _, exists := am.groupPolicyMap[name]; !exists {
		am.mu.Unlock()
		return false
	}
	am.groupPolicyMap[name] = policy
	am.mu.Unlock()

	if err := editGroupPolicyInFile(am.groupFilePath(), name, policy); err != nil {
		slog.Error("AccountManager: failed to edit group policy in file", "name", name, "error", err)
		return false
	}
	if info, err := os.Stat(am.groupFilePath()); err == nil {
		am.mu.Lock()
		am.groupModTime = info.ModTime()
		am.mu.Unlock()
	}
	return true
}
