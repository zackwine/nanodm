package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/zackwine/nanodm"
	"github.com/zackwine/nanodm/source"
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
	source := source.NewSource(log, "testSource", url)
	source.Connect()

	var objects = []nanodm.Object{
		{
			Name:   "Device.DeviceInfo.Version",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeString,
		},
		{
			Name:   "Device.DeviceInfo.Serial",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeString,
		},
	}

	source.Register(objects)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	logrus.Infof("Killed with sig %v", sig)
}
