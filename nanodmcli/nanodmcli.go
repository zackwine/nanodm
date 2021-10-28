package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/zackwine/nanodm"
	"github.com/zackwine/nanodm/source"
)

const (
	VERBOSE = false

	NANODM_URL = "tcp://127.0.0.1:4800"
	SOURCE_URL = "tcp://127.0.0.1:4803"
)

var (
	verbose   = flag.Bool("v", VERBOSE, "If enable let logging level to DEBUG (Normally WARN)")
	nanoURL   = flag.String("n", NANODM_URL, "Nanodm server URL.")
	sourceURL = flag.String("s", SOURCE_URL, "Nanodm source URL.")
)

func main() {

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage %s [flags] <get/set> <path> [<set-value>]:\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()
	if flag.NArg() < 2 {
		flag.Usage()
		os.Exit(1)
	}

	command := flag.Arg(0)
	path := flag.Arg(1)
	setVal := flag.Arg(2)

	logger := logrus.New()
	log := logrus.NewEntry(logger)
	logger.SetFormatter(&logrus.TextFormatter{
		PadLevelText:  true,
		FullTimestamp: true,
		ForceQuote:    true,
	})

	if *verbose {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.ErrorLevel)
	}

	log.Debugf("Starting nanodmcli (%s)", runtime.GOOS)

	if command != "get" && command != "set" && command != "list" {
		fmt.Fprintf(flag.CommandLine.Output(), "Invalid command %s used.  Must be get/set/list.\n\n", command)
		flag.Usage()
		os.Exit(1)
	}

	sourceUUID := uuid.New()
	sourceName := fmt.Sprintf("nanodmcli-%s", sourceUUID.String())

	source := source.NewSource(log.WithField("source", sourceName), sourceName, *nanoURL, *sourceURL, nil)

	// Connect
	err := source.Connect()
	if err != nil {
		log.Errorf("Failed to connect client: %v", err)
		return
	}
	defer func() {
		source.Disconnect()
	}()

	// Call register to update the list of objects the source owns
	err = source.Register(nil)
	if err != nil {
		log.Errorf("Failed to register client: %v", err)
		return
	}

	switch strings.ToLower(command) {
	case "get":
		objects, err := source.GetObjects([]nanodm.Object{{
			Name: path,
		}})
		if err != nil {
			fmt.Printf("{\"error\": \"%v\"}\n", err)
		} else {
			if len(objects) == 1 {
				jsonBytes, err := json.MarshalIndent(objects[0], "", "  ")
				if err != nil {
					fmt.Printf("{\"error\": \"%v\"}\n", err)
				}
				fmt.Printf("%s\n", jsonBytes)
			} else {
				jsonBytes, err := json.MarshalIndent(objects, "", "  ")
				if err != nil {
					fmt.Printf("{\"error\": \"%v\"}\n", err)
				}
				fmt.Printf("%s\n", jsonBytes)
			}

		}

	case "list":
		objects, err := source.ListObjects([]nanodm.Object{{
			Name: path,
		}})
		if err != nil {
			fmt.Printf("{\"error\": \"%v\"}\n", err)
		} else {
			jsonBytes, err := json.MarshalIndent(objects, "", "  ")
			if err != nil {
				fmt.Printf("{\"error\": \"%v\"}\n", err)
			}
			fmt.Printf("%s\n", jsonBytes)
		}

	case "set":
		if flag.NArg() != 3 {
			flag.Usage()
			os.Exit(1)
		}
		objects, err := source.GetObjects([]nanodm.Object{{
			Name: path,
		}})
		if err != nil {
			fmt.Printf("{\"error\": \"%v\"}\n", err)
		} else {
			if len(objects) == 1 {
				setobj := objects[0]
				switch setobj.Type {
				case nanodm.TypeInt, nanodm.TypeLong:
					intVar, err := strconv.ParseInt(setVal, 0, 64)
					if err != nil {
						fmt.Printf("{\"error\": \"%v\"}\n", err)
						return
					}
					setobj.Value = intVar
				case nanodm.TypeUnsignedInt, nanodm.TypeUnsignedLong:
					uintVar, err := strconv.ParseUint(setVal, 0, 64)
					if err != nil {
						fmt.Printf("{\"error\": \"%v\"}\n", err)
						return
					}
					setobj.Value = uintVar
				case nanodm.TypeFloat:
					floatVar, err := strconv.ParseFloat(setVal, 64)
					if err != nil {
						fmt.Printf("{\"error\": \"%v\"}\n", err)
						return
					}
					setobj.Value = floatVar
				default:
					setobj.Value = setVal
				}

				source.SetObject(setobj)
			} else {
				fmt.Printf("{\"error\": \"%v\"}\n", err)
				return
			}
		}

	}

}
