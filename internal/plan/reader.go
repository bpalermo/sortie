package plan

import (
	"bytes"
	"io"
)

func newReader(raw []byte) io.Reader { return bytes.NewReader(raw) }
