package main

import (
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/consul"
	"github.com/docker/libkv/store/etcd"
	"github.com/docker/libkv/store/memo"
	"github.com/docker/libkv/store/zookeeper"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"time"
  )

type Settings struct {
	backend string
	addresses []string
	readers int
	writers int
	keys    int
	payload int
}


func parse() Settings {
	var backend = flag.String("backend", "etcd", "backend name")
	var addresses = flag.String("addresses", "", "addresses to connect to")
	var readers = flag.Int("readers", 1, "number of reader tasks")
	var writers = flag.Int("writers", 0, "number of writer tasks")
	var keys    = flag.Int("keys", 10, "number of keys to use")
	var payload = flag.Int("payload", 10, "size of values")
	flag.Parse()
	return Settings {
		backend: *backend,
		addresses: strings.Split(*addresses, ","),
		readers: *readers,
		writers: *writers,
		keys: *keys,
		payload: *payload,
	}
}

func initialize(s store.Store, keys int, payload int) error {
	value := strings.Repeat("x", payload)
	for i:=0; i < keys; i++ {
		if err := s.Put(fmt.Sprintf("bench/%v", i), []byte(value), nil); err != nil {
			fmt.Printf("initialization error: %v\n", err)
			return err
		}
	}
	return nil
}

func reader(idx int, s store.Store, keys int, n_read *int) {
	for {
		k := rand.Int() % keys
		kn := fmt.Sprintf("bench/%v", k)
		_, err:= s.Get(kn)
		if err != nil {
			fmt.Printf("Get error: %v\n", err)
		}
		*n_read++
	}
}

func writer(idx int, s store.Store, keys int, payload int, n_write *int) {
	value := strings.Repeat("x", payload)
	for {
		k := rand.Int() % keys
		kn := fmt.Sprintf("bench/%v", k)
		err := s.Put(kn, []byte(value), nil)
		if err != nil {
			fmt.Printf("Put error: %v\n", err)
		}
		*n_write++
	}
}

func execute(settings Settings) {
	tasks := settings.readers + settings.writers
	var stores []store.Store
	for i:=0; i < tasks; i++ {
		s, err := libkv.NewStore(store.Backend(settings.backend), settings.addresses,
			&store.Config { PersistConnection: true})
		if err != nil {
			fmt.Printf("initialization error:%v\n", err)
			return
		}
		stores = append(stores, s)
	}
	if err := initialize(stores[0], settings.keys, settings.payload); err != nil {
		return
	}
	n_read := 0
	n_write := 0
	for i:=0; i < settings.readers; i++ {
		go reader(i, stores[i], settings.keys, &n_read)
	}
	for i:=0; i < settings.writers; i++ {
		go writer(i, stores[i+settings.readers], settings.keys, settings.payload, &n_write)
	}
	for {
		time.Sleep(10 * time.Second)
		fmt.Printf("%v rps  %v wps\n", n_read/10, n_write/10)
		n_read = 0
		n_write = 0
	}
}

func main() {
	consul.Register()
	etcd.Register()
	memo.Register()
	zookeeper.Register()
	settings := parse()
	fmt.Printf("Executing bench with %v\n", settings)
	execute(settings)
}