package service

import (
	"github.com/zbum/scouter-server-go/internal/login"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterAccountHandlers registers account management TCP handlers.
func RegisterAccountHandlers(r *Registry, accountManager *login.AccountManager) {

	// ADD_ACCOUNT: create a new account.
	r.Register(protocol.ADD_ACCOUNT, func(din *protocol.DataInputX, dout *protocol.DataOutputX, loggedIn bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		m := pk.(*pack.MapPack)

		acct := &login.Account{
			ID:       m.GetText("id"),
			Password: m.GetText("pass"),
			Email:    m.GetText("email"),
			Group:    m.GetText("group"),
		}

		ok := accountManager.AddAccount(acct)

		result := &pack.MapPack{}
		result.Put("result", &value.BooleanValue{Value: ok})
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})

	// EDIT_ACCOUNT: update an existing account.
	r.Register(protocol.EDIT_ACCOUNT, func(din *protocol.DataInputX, dout *protocol.DataOutputX, loggedIn bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		m := pk.(*pack.MapPack)

		acct := &login.Account{
			ID:       m.GetText("id"),
			Password: m.GetText("pass"),
			Email:    m.GetText("email"),
			Group:    m.GetText("group"),
		}

		ok := accountManager.EditAccount(acct)

		result := &pack.MapPack{}
		result.Put("result", &value.BooleanValue{Value: ok})
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})

	// CHECK_ACCOUNT_ID: check if an account ID is available.
	r.Register(protocol.CHECK_ACCOUNT_ID, func(din *protocol.DataInputX, dout *protocol.DataOutputX, loggedIn bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		m := pk.(*pack.MapPack)
		id := m.GetText("id")

		ok := accountManager.AvailableID(id)

		result := &pack.MapPack{}
		result.Put("result", &value.BooleanValue{Value: ok})
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})

	// LIST_ACCOUNT: return all accounts as BlobValue streams.
	// Client sends null param and reads each response via readValue() (not readPack).
	r.Register(protocol.LIST_ACCOUNT, func(din *protocol.DataInputX, dout *protocol.DataOutputX, loggedIn bool) {
		accounts := accountManager.GetAccountList()
		for _, acct := range accounts {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			value.WriteValue(dout, &value.BlobValue{Value: acct.ToBytes()})
		}
	})

	// LIST_ACCOUNT_GROUP: return all group names.
	// Client sends null param.
	r.Register(protocol.LIST_ACCOUNT_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, loggedIn bool) {

		groups := accountManager.GetGroupList()

		result := &pack.MapPack{}
		lv := value.NewListValue()
		for _, g := range groups {
			lv.Value = append(lv.Value, value.NewTextValue(g))
		}
		result.Put("group_list", lv)

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})

	// GET_GROUP_POLICY_ALL: return all group policies.
	// Client sends null param.
	r.Register(protocol.GET_GROUP_POLICY_ALL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, loggedIn bool) {

		allPolicies := accountManager.GetAllGroupPolicies()

		result := &pack.MapPack{}
		for name, mv := range allPolicies {
			result.Put(name, mv)
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})

	// EDIT_GROUP_POLICY: update a group's policy.
	r.Register(protocol.EDIT_GROUP_POLICY, func(din *protocol.DataInputX, dout *protocol.DataOutputX, loggedIn bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		m := pk.(*pack.MapPack)

		groupName := m.GetText("name")
		policyVal := m.Get("policy")

		ok := false
		if mv, isMV := policyVal.(*value.MapValue); isMV {
			ok = accountManager.EditGroupPolicy(groupName, mv)
		}

		result := &pack.MapPack{}
		result.Put("result", &value.BooleanValue{Value: ok})
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})

	// ADD_ACCOUNT_GROUP: create a new account group.
	r.Register(protocol.ADD_ACCOUNT_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, loggedIn bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		m := pk.(*pack.MapPack)

		groupName := m.GetText("name")
		policyVal := m.Get("policy")

		var mv *value.MapValue
		if v, ok := policyVal.(*value.MapValue); ok {
			mv = v
		} else {
			mv = value.NewMapValue()
		}

		ok := accountManager.AddAccountGroup(groupName, mv)

		result := &pack.MapPack{}
		result.Put("result", &value.BooleanValue{Value: ok})
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})
}
