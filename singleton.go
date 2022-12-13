package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

// Singleton makes sure there is only one "single" instance across the system.
// by binding to a tcp resource as specified by addr, eg. "127.0.0.1:51337".
func Singleton(addr string) {
	go singletonServer(addr)
	for {
		// wait and confirm that server was started successfully
		pid, err := getSingletonPID(addr)
		if err == nil && pid == strconv.Itoa(os.Getpid()) {
			return
		}
		time.Sleep(1 * time.Second)
	}
}

func singletonServer(addr string) {
	// Listen for incoming connections.
	http.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, strconv.Itoa(os.Getpid()))
	})
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		pid, err := getSingletonPID(addr)
		if err != nil {
			log.Fatalln("agent already running, error on retriving pid", err)
		}

		log.Fatalln("agent already running on pid", pid)
	}
}

func getSingletonPID(addr string) (string, error) {
	resp, err := http.Get("http://" + addr + "/")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
