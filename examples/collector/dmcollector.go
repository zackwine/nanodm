package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/zackwine/nanodm"
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

	<-time.After(10 * time.Second)

	objects, errs := server.Get([]string{"Device.DeviceInfo.Version"})
	if len(errs) > 0 {
		log.Errorf("Failed to get object: %v", errs)
	}
	log.Infof("%+v", objects)

	<-time.After(1 * time.Second)

	err := server.Set(nanodm.Object{
		Name:  "Device.DeviceInfo.Version",
		Value: "0.0.0",
	})
	if err != nil {
		log.Errorf("Failed to set object: %v", err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	logrus.Infof("Killed with sig %v", sig)

}
