package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
)

func main() {
	log.Printf("⬅🖐 leftshove started")
	Singleton("127.0.0.1:51337")
	CtrlC()

	var confFile string
	flag.StringVar(&confFile, "config", "./sample.env", "configuration .env file location (ie. './config.env')")
	seedFlag := flag.Bool("seed", false, "seed nms db")
	cdcFlag := flag.Bool("cdc", false, "seed nms db")
	runOnce := flag.Bool("runonce", false, "run and capture source only once")
	bqGenFlag := flag.Bool("bq", false, "generate bigquery table schemas")
	flag.Parse()

	err := godotenv.Load(confFile)
	if err != nil {
		log.Fatal("error loading .env file")
	}

	if *seedFlag {
		fmt.Printf("seed nms db: %v\n", *seedFlag)
		err := seedNMSdb()
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
	}
	if *bqGenFlag {
		err := createBQtables()
		if err != nil {
			log.Println(err)
			// os.Exit(2)
		}
	}
	if *cdcFlag {
		fmt.Printf("cdc: %v\n", *cdcFlag)
		if *runOnce {
			err := cdc()
			if err != nil {
				log.Println(err)
				os.Exit(3)
			}
		} else {
			for {
				err := cdc()
				if err != nil {
					log.Println(err)
					os.Exit(4)
				}
			}
		}

	}
	log.Printf("End")
}

// CtrlC intercepts any Ctrl+C keyboard input and exits to the shell.
func CtrlC() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Printf("End")
		fmt.Fprintf(os.Stdout, " ⬅🖐👋\n")
		os.Exit(5)
	}()
}
