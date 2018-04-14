package fqueue

import "testing"

func TestNewFilePartition(t *testing.T) {
	part, err := NewFilePartition("G:\\test\\fqueue")
	if err != nil {
		t.Fatal(err)
	}
	_ = part
	//msg :=
	//part.WriteMsg()
}