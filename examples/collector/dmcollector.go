package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/zackwine/nanodm/collector"
)

func main() {

	url := "tcp://127.0.0.1:4500"

	logrus.SetFormatter(&logrus.TextFormatter{
		PadLevelText:  true,
		FullTimestamp: true,
		ForceQuote:    true,
	})
	logrus.SetLevel(logrus.DebugLevel)
	log := logrus.NewEntry(logrus.New())

	log.Info("Starting dmcollector example...")
	server := collector.NewServer(log, url)
	server.Start()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	logrus.Infof("Killed with sig %v", sig)

}
