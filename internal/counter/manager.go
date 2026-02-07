package counter

import (
	"encoding/xml"
	"fmt"
	"log/slog"
	"sync"

	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// TagObjDetectedType is the tag key agents use to indicate the reference type
// for dynamically detected object types (matches Java's ScouterConstants.TAG_OBJ_DETECTED_TYPE).
const TagObjDetectedType = "detected"

// ObjectTypeInfo holds information about a registered object type.
type ObjectTypeInfo struct {
	Name      string
	Family    string
	DispName  string
	Icon      string
	SubObject bool
}

// ObjectTypeManager tracks known and dynamically registered object types.
// It parses the embedded counters.xml at startup and registers new types
// when agents with unknown types send heartbeats.
type ObjectTypeManager struct {
	mu            sync.RWMutex
	knownTypes    map[string]*ObjectTypeInfo // from counters.xml
	customTypes   map[string]*ObjectTypeInfo // dynamically added
	familyMasters map[string]string          // family name -> master counter name
	customDirty   bool
	customXML     []byte
}

// NewObjectTypeManager creates a new manager, parsing the embedded counters.xml.
func NewObjectTypeManager() *ObjectTypeManager {
	m := &ObjectTypeManager{
		knownTypes:    make(map[string]*ObjectTypeInfo),
		customTypes:   make(map[string]*ObjectTypeInfo),
		familyMasters: make(map[string]string),
	}
	m.parseDefaultXML()
	return m
}

// xmlCounters is used to parse the counters.xml structure.
type xmlCounters struct {
	XMLName  xml.Name       `xml:"Counters"`
	Familys  xmlFamilys     `xml:"Familys"`
	Types    xmlObjectTypes `xml:"Types"`
}

type xmlFamilys struct {
	Families []xmlFamily `xml:"Family"`
}

type xmlFamily struct {
	Name   string `xml:"name,attr"`
	Master string `xml:"master,attr"`
}

type xmlObjectTypes struct {
	ObjectTypes []xmlObjectType `xml:"ObjectType"`
}

type xmlObjectType struct {
	Name      string `xml:"name,attr"`
	Family    string `xml:"family,attr"`
	Disp      string `xml:"disp,attr"`
	Icon      string `xml:"icon,attr"`
	SubObject string `xml:"sub-object,attr"`
}

func (m *ObjectTypeManager) parseDefaultXML() {
	var counters xmlCounters
	if err := xml.Unmarshal(DefaultCountersXML, &counters); err != nil {
		slog.Error("failed to parse counters.xml", "error", err)
		return
	}

	for _, f := range counters.Familys.Families {
		if f.Master != "" {
			m.familyMasters[f.Name] = f.Master
		}
	}
	slog.Info("ObjectTypeManager loaded families", "count", len(m.familyMasters))

	for _, ot := range counters.Types.ObjectTypes {
		m.knownTypes[ot.Name] = &ObjectTypeInfo{
			Name:      ot.Name,
			Family:    ot.Family,
			DispName:  ot.Disp,
			Icon:      ot.Icon,
			SubObject: ot.SubObject == "true",
		}
	}

	slog.Info("ObjectTypeManager loaded known types", "count", len(m.knownTypes))
}

// AddObjectTypeIfNotExist checks if the given objType is known; if not,
// it uses the "detected" tag from the agent's tags to create a new type
// inheriting family/icon from the detected reference type.
// Returns true if a new type was registered.
func (m *ObjectTypeManager) AddObjectTypeIfNotExist(objType string, tags *value.MapValue) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Already known?
	if _, ok := m.knownTypes[objType]; ok {
		return false
	}
	if _, ok := m.customTypes[objType]; ok {
		return false
	}

	// Get detected reference type from tags
	detected := ""
	if tags != nil {
		if v, ok := tags.Get(TagObjDetectedType); ok {
			if tv, ok := v.(*value.TextValue); ok {
				detected = tv.Value
			}
		}
	}

	// Find the reference type
	refType := m.knownTypes[detected]
	if refType == nil {
		refType = m.customTypes[detected]
	}
	if refType == nil {
		// No reference type found - cannot register
		return false
	}

	// Create and register new custom type
	icon := refType.Name
	if refType.Icon != "" {
		icon = refType.Icon
	}
	m.customTypes[objType] = &ObjectTypeInfo{
		Name:      objType,
		Family:    refType.Family,
		DispName:  objType,
		Icon:      icon,
		SubObject: refType.SubObject,
	}
	m.customDirty = true

	slog.Info("Registered new object type",
		"objType", objType,
		"detected", detected,
		"family", refType.Family)

	return true
}

// GetCustomXML returns XML bytes containing dynamically registered custom types.
// Returns nil if there are no custom types.
func (m *ObjectTypeManager) GetCustomXML() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.customTypes) == 0 {
		return nil
	}

	if m.customDirty || m.customXML == nil {
		m.rebuildCustomXML()
		m.customDirty = false
	}

	return m.customXML
}

func (m *ObjectTypeManager) rebuildCustomXML() {
	var xml string
	xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Counters>\n\t<Types>\n"
	for _, ct := range m.customTypes {
		subObj := ""
		if ct.SubObject {
			subObj = " sub-object=\"true\""
		}
		icon := ""
		if ct.Icon != "" {
			icon = fmt.Sprintf(" icon=\"%s\"", ct.Icon)
		}
		xml += fmt.Sprintf("\t\t<ObjectType name=\"%s\" family=\"%s\" disp=\"%s\"%s%s />\n",
			ct.Name, ct.Family, ct.DispName, icon, subObj)
	}
	xml += "\t</Types>\n</Counters>\n"
	m.customXML = []byte(xml)
}

// GetMasterCounter returns the master counter name for the given object type.
// Returns empty string if not found.
func (m *ObjectTypeManager) GetMasterCounter(objType string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var family string
	if info, ok := m.knownTypes[objType]; ok {
		family = info.Family
	} else if info, ok := m.customTypes[objType]; ok {
		family = info.Family
	}
	if family == "" {
		return ""
	}
	return m.familyMasters[family]
}

// IsKnownType returns true if the given type is known (standard or custom).
func (m *ObjectTypeManager) IsKnownType(objType string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.knownTypes[objType]; ok {
		return true
	}
	_, ok := m.customTypes[objType]
	return ok
}
