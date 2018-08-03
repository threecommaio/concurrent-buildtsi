package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type Database struct {
	Name   string
	Shards []int
}

var (
	datadir        string
	waldir         string
	maxLogFileSize int
	concurrency    int
	verbose        bool
	databases      []*Database
)

func buildsiRun(wg *sync.WaitGroup, done chan bool, database string, shard int) {
	defer wg.Done()

	log.Printf("Processing (%s) on shard (%d)\n", database, shard)

	cmd := exec.Command("influx_inspect", "buildtsi",
		"-datadir", datadir,
		"-waldir", waldir,
		"-max-log-file-size", strconv.Itoa(maxLogFileSize),
		"-database", database,
		"-shard", strconv.Itoa(shard))

	cmd.Stdin = strings.NewReader("y")
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("cmd.Run() failed with %s\n", err)
	}

	if verbose {
		fmt.Println(string(out))
	}
	<-done
}

func main() {
	flag.StringVar(&datadir, "datadir", "/var/lib/influxdb/data", "datadir location")
	flag.StringVar(&waldir, "waldir", "/var/lib/influxdb/wal", "waldir location")
	flag.IntVar(&maxLogFileSize, "max-log-file-size", 131072, "max-log-file-size")
	flag.IntVar(&concurrency, "concurrency", runtime.GOMAXPROCS(0), "concurrency")
	flag.BoolVar(&verbose, "verbose", false, "enable verbose mode that prints out stdout from [influx_inspect]")
	flag.Parse()

	path, err := ioutil.ReadDir(datadir)
	if err != nil {
		log.Fatal(err)
	}

	for _, db := range path {
		if db.IsDir() && db.Name() != "_internal" {
			databases = append(databases, &Database{Name: db.Name()})
		}
	}

	for _, database := range databases {
		shards, err := ioutil.ReadDir(filepath.Join(datadir, database.Name, "autogen"))
		if err != nil {
			log.Fatal(err)
		}

		for _, shard := range shards {
			if shard.IsDir() {
				shardNum, _ := strconv.Atoi(shard.Name())
				database.Shards = append(database.Shards, shardNum)
			}
		}
	}

	bufCh := make(chan bool, concurrency)
	wg := new(sync.WaitGroup)
	for _, db := range databases {
		for _, shard := range db.Shards {
			bufCh <- true
			wg.Add(1)
			go buildsiRun(wg, bufCh, db.Name, shard)
		}
	}
	close(bufCh)
	wg.Wait()
}
