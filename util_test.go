package fqueue

import (
	"fmt"
	"golang.org/x/exp/mmap"
	"os"
	"testing"
)

func TestHomePath(t *testing.T) {
	p := HomePath()
	t.Log(p)
}

func TestMMap(t *testing.T) {
	name := "G:\\temp.txt"
	writer, err := os.OpenFile(name, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		t.Fatal(err)
	}
	readAt, err := mmap.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	var offset int64 = 0
	for i := 0; i < 100; i++ {
		str := fmt.Sprintf("line-%d\n", i)
		writer.WriteString(str)
		text := make([]byte, len(str))
		n, err := readAt.ReadAt(text, offset)
		offset += int64(n)
		if err != nil {
			t.Error(err)
			continue
		}
		fmt.Println("len: ", n, " res: ", string(text))
	}
	writer.Close()
	readAt.Close()
}
