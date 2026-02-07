package login

import (
	"encoding/xml"
	"os"
	"strings"

	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// --- account.xml structures ---

type xmlAccounts struct {
	XMLName  xml.Name     `xml:"Accounts"`
	Accounts []xmlAccount `xml:"Account"`
}

type xmlAccount struct {
	ID    string `xml:"id,attr"`
	Pass  string `xml:"pass,attr"`
	Group string `xml:"group,attr"`
	Email string `xml:"Email"`
}

// parseAccountFile parses account.xml into a map of Account keyed by ID.
func parseAccountFile(path string) (map[string]*Account, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc xmlAccounts
	if err := xml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	m := make(map[string]*Account, len(doc.Accounts))
	for _, a := range doc.Accounts {
		m[a.ID] = &Account{
			ID:       a.ID,
			Password: a.Pass,
			Email:    a.Email,
			Group:    a.Group,
		}
	}
	return m, nil
}

// addAccountToFile appends a new Account element to account.xml.
func addAccountToFile(path string, acct *Account) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc xmlAccounts
	if err := xml.Unmarshal(data, &doc); err != nil {
		return err
	}
	doc.Accounts = append(doc.Accounts, xmlAccount{
		ID:    acct.ID,
		Pass:  acct.Password,
		Group: acct.Group,
		Email: acct.Email,
	})
	return writeAccountFile(path, &doc)
}

// editAccountInFile updates an existing Account element in account.xml.
func editAccountInFile(path string, acct *Account) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc xmlAccounts
	if err := xml.Unmarshal(data, &doc); err != nil {
		return err
	}
	for i := range doc.Accounts {
		if doc.Accounts[i].ID == acct.ID {
			doc.Accounts[i].Pass = acct.Password
			doc.Accounts[i].Group = acct.Group
			doc.Accounts[i].Email = acct.Email
			break
		}
	}
	return writeAccountFile(path, &doc)
}

func writeAccountFile(path string, doc *xmlAccounts) error {
	out, err := xml.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	content := xml.Header + string(out) + "\n"
	return os.WriteFile(path, []byte(content), 0644)
}

// --- account_group.xml structures ---

type xmlGroups struct {
	XMLName xml.Name   `xml:"Groups"`
	Groups  []xmlGroup `xml:"Group"`
}

type xmlGroup struct {
	Name   string    `xml:"name,attr"`
	Policy xmlPolicy `xml:"Policy"`
}

type xmlPolicy struct {
	AllowEditGroupPolicy  string `xml:"AllowEditGroupPolicy"`
	AllowHeapDump         string `xml:"AllowHeapDump"`
	AllowFileDump         string `xml:"AllowFileDump"`
	AllowHeapHistogram    string `xml:"AllowHeapHistogram"`
	AllowThreadDump       string `xml:"AllowThreadDump"`
	AllowSystemGC         string `xml:"AllowSystemGC"`
	AllowConfigure        string `xml:"AllowConfigure"`
	AllowExportCounter    string `xml:"AllowExportCounter"`
	AllowExportAppSum     string `xml:"AllowExportAppSum"`
	AllowLoginList        string `xml:"AllowLoginList"`
	AllowDBManager        string `xml:"AllowDBManager"`
	AllowAddAccount       string `xml:"AllowAddAccount"`
	AllowEditAccount      string `xml:"AllowEditAccount"`
	AllowSqlParameter     string `xml:"AllowSqlParameter"`
	AllowKillTransaction  string `xml:"AllowKillTransaction"`
	AllowExportClass      string `xml:"AllowExportClass"`
	AllowRedefineClass    string `xml:"AllowRedefineClass"`
	AllowDefineObjectType string `xml:"AllowDefineObjectType"`
}

// policyFieldNames lists all 18 policy field names in order.
var policyFieldNames = []string{
	"AllowEditGroupPolicy",
	"AllowHeapDump",
	"AllowFileDump",
	"AllowHeapHistogram",
	"AllowThreadDump",
	"AllowSystemGC",
	"AllowConfigure",
	"AllowExportCounter",
	"AllowExportAppSum",
	"AllowLoginList",
	"AllowDBManager",
	"AllowAddAccount",
	"AllowEditAccount",
	"AllowSqlParameter",
	"AllowKillTransaction",
	"AllowExportClass",
	"AllowRedefineClass",
	"AllowDefineObjectType",
}

func policyToMap(p *xmlPolicy) map[string]string {
	return map[string]string{
		"AllowEditGroupPolicy":  p.AllowEditGroupPolicy,
		"AllowHeapDump":         p.AllowHeapDump,
		"AllowFileDump":         p.AllowFileDump,
		"AllowHeapHistogram":    p.AllowHeapHistogram,
		"AllowThreadDump":       p.AllowThreadDump,
		"AllowSystemGC":         p.AllowSystemGC,
		"AllowConfigure":        p.AllowConfigure,
		"AllowExportCounter":    p.AllowExportCounter,
		"AllowExportAppSum":     p.AllowExportAppSum,
		"AllowLoginList":        p.AllowLoginList,
		"AllowDBManager":        p.AllowDBManager,
		"AllowAddAccount":       p.AllowAddAccount,
		"AllowEditAccount":      p.AllowEditAccount,
		"AllowSqlParameter":     p.AllowSqlParameter,
		"AllowKillTransaction":  p.AllowKillTransaction,
		"AllowExportClass":      p.AllowExportClass,
		"AllowRedefineClass":    p.AllowRedefineClass,
		"AllowDefineObjectType": p.AllowDefineObjectType,
	}
}

func mapToPolicy(m map[string]string) xmlPolicy {
	return xmlPolicy{
		AllowEditGroupPolicy:  m["AllowEditGroupPolicy"],
		AllowHeapDump:         m["AllowHeapDump"],
		AllowFileDump:         m["AllowFileDump"],
		AllowHeapHistogram:    m["AllowHeapHistogram"],
		AllowThreadDump:       m["AllowThreadDump"],
		AllowSystemGC:         m["AllowSystemGC"],
		AllowConfigure:        m["AllowConfigure"],
		AllowExportCounter:    m["AllowExportCounter"],
		AllowExportAppSum:     m["AllowExportAppSum"],
		AllowLoginList:        m["AllowLoginList"],
		AllowDBManager:        m["AllowDBManager"],
		AllowAddAccount:       m["AllowAddAccount"],
		AllowEditAccount:      m["AllowEditAccount"],
		AllowSqlParameter:     m["AllowSqlParameter"],
		AllowKillTransaction:  m["AllowKillTransaction"],
		AllowExportClass:      m["AllowExportClass"],
		AllowRedefineClass:    m["AllowRedefineClass"],
		AllowDefineObjectType: m["AllowDefineObjectType"],
	}
}

// parseGroupFile parses account_group.xml into a map of group name → MapValue.
func parseGroupFile(path string) (map[string]*value.MapValue, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc xmlGroups
	if err := xml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	m := make(map[string]*value.MapValue, len(doc.Groups))
	for _, g := range doc.Groups {
		pm := policyToMap(&g.Policy)
		mv := value.NewMapValue()
		for _, name := range policyFieldNames {
			mv.Put(name, &value.BooleanValue{Value: strings.EqualFold(pm[name], "true")})
		}
		m[g.Name] = mv
	}
	return m, nil
}

// addGroupToFile appends a new Group element to account_group.xml.
func addGroupToFile(path string, name string, policy *value.MapValue) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc xmlGroups
	if err := xml.Unmarshal(data, &doc); err != nil {
		return err
	}

	pm := make(map[string]string)
	for _, fn := range policyFieldNames {
		pm[fn] = "false"
	}
	if policy != nil {
		for _, fn := range policyFieldNames {
			if v, ok := policy.Get(fn); ok {
				if bv, ok := v.(*value.BooleanValue); ok && bv.Value {
					pm[fn] = "true"
				}
			}
		}
	}

	doc.Groups = append(doc.Groups, xmlGroup{
		Name:   name,
		Policy: mapToPolicy(pm),
	})
	return writeGroupFile(path, &doc)
}

// editGroupPolicyInFile updates Policy elements for a group in account_group.xml.
func editGroupPolicyInFile(path string, name string, policy *value.MapValue) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc xmlGroups
	if err := xml.Unmarshal(data, &doc); err != nil {
		return err
	}

	for i := range doc.Groups {
		if doc.Groups[i].Name == name {
			pm := policyToMap(&doc.Groups[i].Policy)
			if policy != nil {
				for _, fn := range policyFieldNames {
					if v, ok := policy.Get(fn); ok {
						if bv, ok := v.(*value.BooleanValue); ok {
							if bv.Value {
								pm[fn] = "true"
							} else {
								pm[fn] = "false"
							}
						}
					}
				}
			}
			doc.Groups[i].Policy = mapToPolicy(pm)
			break
		}
	}
	return writeGroupFile(path, &doc)
}

func writeGroupFile(path string, doc *xmlGroups) error {
	out, err := xml.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	content := xml.Header + string(out) + "\n"
	return os.WriteFile(path, []byte(content), 0644)
}
