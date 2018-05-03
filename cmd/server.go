package cmd

import (
	"os"
	"os/signal"
	"syscall"
	log "github.com/sirupsen/logrus"
)

const (
	VERSION = 1
)

func waitSignal() {
	var sigChan = make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)
	for sig := range sigChan {
		if sig == syscall.SIGHUP {
			log.Info("sighup")
		} else {
			log.Fatalf("wrong signal, code: %v", sig)
		}
	}
}