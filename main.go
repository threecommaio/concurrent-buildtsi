package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
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
	version        = "0.0.0"
	datadir        string
	waldir         string
	maxLogFileSize int
	concurrency    int
	verbose        bool
	database       string
	shards         string
	databases      []*Database
	shardsOpt      []string
)

func buildsiRun(wg *sync.WaitGroup, done chan bool, database string, shard int) {
	defer wg.Done()

	log.Printf("Processing (%s) on shard (%d)\n", database, shard)

	// exec influx inspect
	cmd := exec.Command("influx_inspect", "buildtsi",
		"-datadir", datadir,
		"-waldir", waldir,
		"-max-log-file-size", strconv.Itoa(maxLogFileSize),
		"-database", database,
		"-shard", strconv.Itoa(shard))

	// pass 'y' to influx inspect command
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

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func main() {
	flag.StringVar(&datadir, "datadir", "/var/lib/influxdb/data", "datadir location")
	flag.StringVar(&waldir, "waldir", "/var/lib/influxdb/wal", "waldir location")
	flag.IntVar(&maxLogFileSize, "max-log-file-size", 131072, "max-log-file-size")
	flag.IntVar(&concurrency, "concurrency", runtime.GOMAXPROCS(0), "concurrency")
	flag.BoolVar(&verbose, "verbose", false, "enable verbose mode that prints out stdout from [influx_inspect]")
	flag.StringVar(&database, "database", "", "run on a specific database (optional)")
	flag.StringVar(&shards, "shards", "", "run on a specific set of shards (optional)")
	v := flag.Bool("version", false, "prints current version")

	flag.Parse()

	if *v {
		fmt.Println(version)
		os.Exit(0)
	}

	if shards != "" {
		shardsOpt = strings.Split(shards, ",")
	}

	// read datadir for databases
	path, err := ioutil.ReadDir(datadir)
	if err != nil {
		log.Fatal(err)
	}

	// check if database is optionally passed and use that database if needed
	if database != "" {
		databases = append(databases, &Database{Name: database})
	} else {
		// iterate over databases on the filesystem and collect those
		for _, db := range path {
			if db.IsDir() && db.Name() != "_internal" {
				databases = append(databases, &Database{Name: db.Name()})
			}
		}
	}

	// iterate over the databases and find shards
	for _, database := range databases {
		shardsFs, err := ioutil.ReadDir(filepath.Join(datadir, database.Name, "autogen"))
		if err != nil {
			log.Fatal(err)
		}

		// check if shards were optionally passed, if not find them on filesystem
		if shardsOpt == nil {
			// find shards on filesystem for the database
			for _, shard := range shardsFs {
				if shard.IsDir() {
					shardNum, _ := strconv.Atoi(shard.Name())
					database.Shards = append(database.Shards, shardNum)
				}
			}
		} else {
			// use the defined shards from command-line
			for _, shard := range shardsOpt {
				shardNum, _ := strconv.Atoi(shard)
				database.Shards = append(database.Shards, shardNum)
			}
		}
	}

	// setup a buffered channel with limited concurrency
	bufCh := make(chan bool, concurrency)
	wg := new(sync.WaitGroup)

	// iterate over database + shards and launch influx inspect, capping it to concurrency
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
