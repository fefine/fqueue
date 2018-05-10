package main

import (
	"flag"
	"fmt"
	"github.com/fefine/fqueue"
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
	flag.StringVar(&config.DataPath, "path", "", "data path")
	flag.StringVar(&config.ListenerAddress, "address", "127.0.0.1:8090", "listen address")
	flag.StringVar(&endpoints, "endpoints", "", "listen address")
	flag.Parse()

	if endpoints == "" {
		log.Fatal("must provide etcd endpoints")
	}

	config.EtcdEndPoints = strings.Split(endpoints, ",")
	config.Debug = true
	if config.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if config.DataPath == "" {
		config.DataPath = fmt.Sprintf("%s/%s/fqueue", fqueue.HomePath(), config.Name)
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
