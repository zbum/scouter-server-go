package login

import _ "embed"

//go:embed account.xml
var defaultAccountXML []byte

//go:embed account_group.xml
var defaultAccountGroupXML []byte
