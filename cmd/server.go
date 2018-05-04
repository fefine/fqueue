package main

import (
	"flag"
	"fmt"
	"fqueue"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	var config fqueue.BrokerConfig
	var printVal bool
	var endpoints string

	flag.BoolVar(&printVal, "version", false, "print version")
	flag.BoolVar(&config.Debug, "debug", false, "debug mode")
	flag.StringVar(&config.Name, "name", "broker-1", "broker name")
	flag.StringVar(&config.DataPath, "path", fmt.Sprintf("%s/fqueue", fqueue.HomePath()), "data path")
	flag.StringVar(&config.ListenerAddress, "a", ":8090", "listen address")
	flag.StringVar(&endpoints, "endpoints", "192.168.1.121:2379", "listen address")
	config.EtcdEndPoints = strings.Split(endpoints, ",")

	config.Debug = true

	if config.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	broker, err := fqueue.NewBrokerAndStart(&config)
	if err != nil {
		log.Fatal("start server failed")
	}
	defer broker.Close()
	waitSignal()
}

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
