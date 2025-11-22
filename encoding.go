package mycelia

// PayloadEncoding denotes which structure the payload bytes should be treated
// as. This is entirely for consumer convenience and contributes nothing to the
// inner workings of the actual protocol.
type PayloadEncoding uint8

const (
	EncodingNA = iota // NA should be first, just in case ordering changes.
	EncodingJson
	EncodingXml
	EncodingYaml
	EncodingCsv
	EncodingToml
	EncodingIni
	EncodingProtobuf
)

var EncodingName = map[PayloadEncoding]string{
	EncodingJson:     "json",
	EncodingXml:      "xml",
	EncodingYaml:     "yaml",
	EncodingCsv:      "csv",
	EncodingToml:     "toml",
	EncodingIni:      "ini",
	EncodingProtobuf: "protobuf",
	EncodingNA:       "na",
}

func (pe PayloadEncoding) String() string {
	return EncodingName[pe]
}
