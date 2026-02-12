package tagcnt

// Tag group/key definitions matching Java's TagCountConfig.
const (
	TagGroupService = "service"
	TagGroupError   = "error"
	TagGroupAlert   = "alert"

	TagKeyTotal   = "total"
	TagKeyService = "service"
	TagKeyError   = "error"
	TagKeyIP      = "ip"
	TagKeyUA      = "ua"
)

// TagDef defines a tag counting dimension.
type TagDef struct {
	Group string
	Key   string
}

// DefaultTagDefs returns the default tag definitions for XLog processing.
func DefaultTagDefs() []TagDef {
	return []TagDef{
		{TagGroupService, TagKeyTotal},
		{TagGroupService, TagKeyService},
		{TagGroupError, TagKeyTotal},
		{TagGroupError, TagKeyError},
	}
}
