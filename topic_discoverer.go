package main

import (
	"log"
	"os"
	"regexp"
	"time"

	"github.com/bitly/nsq/util/lookupd"
)

type TopicDiscovererFunc func(topic string) error

type TopicDiscovererConfig struct {
	LookupdAddresses []string
	Pattern          string
	Refresh          time.Duration
	Handler          TopicDiscovererFunc
	Logger           *log.Logger
}

type TopicDiscoverer struct {
	TopicDiscovererConfig
	topics   map[string]bool
	termChan chan os.Signal
}

func NewTopicDiscoverer(cfg TopicDiscovererConfig) (*TopicDiscoverer, error) {
	r := &TopicDiscoverer{
		TopicDiscovererConfig: cfg,
		termChan:              make(chan os.Signal),
		topics:                make(map[string]bool),
	}

	if r.Logger == nil {
		r.Logger = log.New(os.Stdout, "[topic_discoverer]: ", log.LstdFlags)
	}

	return r, nil
}

func (t *TopicDiscoverer) allowTopicName(pattern, name string) bool {
	match, err := regexp.MatchString(pattern, name)
	if err != nil {
		return false
	}
	return match
}

func (t *TopicDiscoverer) syncTopics() {
	newTopics, err := lookupd.GetLookupdTopics(t.LookupdAddresses)
	if err != nil {
		t.Logger.Printf("ERROR: could not retrieve topic list: %s", err)
		return
	}

	for _, topic := range newTopics {
		if _, ok := t.topics[topic]; !ok {
			if !t.allowTopicName(t.Pattern, topic) {
				t.Logger.Println("Skipping topic ", topic)
				continue
			}
			err := t.Handler(topic)
			if err != nil {
				t.Logger.Printf("ERROR: could not register topic %s: %s", topic, err)
			} else {
				t.topics[topic] = true
			}
		}
	}
}

func (t *TopicDiscoverer) Start() {
	ticker := time.Tick(t.Refresh)
	t.syncTopics()

	for {
		select {
		case <-ticker:
			t.syncTopics()
		case <-t.termChan:
			return
		}
	}
}

func (t *TopicDiscoverer) Signal(sig os.Signal) {
	t.termChan <- sig
}
