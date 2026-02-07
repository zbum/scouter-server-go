package counter

import _ "embed"

//go:embed counters.xml
var DefaultCountersXML []byte
