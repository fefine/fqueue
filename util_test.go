package fqueue

import (
	"testing"
)

func TestHomePath(t *testing.T) {
	p := HomePath()
	t.Log(p)
}
