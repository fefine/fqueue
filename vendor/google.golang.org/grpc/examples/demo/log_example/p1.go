package log_example

import log "github.com/sirupsen/logrus"

func init()  {
	log.SetFormatter(&log.TextFormatter{})
	log.SetLevel(log.DebugLevel)
}

func Demo1() {
	log.Debug("log1")
}

func Demo2() {
	log.Info("log2")
}
