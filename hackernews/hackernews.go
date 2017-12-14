package main

import (
	"fmt"

	"os"
	"strconv"
	"time"

	"github.com/ngaut/log"

	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/kabergstrom/site/protocol"
	"github.com/kabergstrom/site/protocol/subjects"
	nats "github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"gopkg.in/zabawaba99/firego.v1"
)

type hnPost struct {
	ID          int64   `json:"id"`
	Deleted     bool    `json:"deleted"`
	TypeStr     string  `json:"type"`
	By          string  `json:"by"`
	Time        int32   `json:"time"`
	Text        string  `json:"text"`
	Dead        bool    `json:"dead"`
	Parent      int64   `json:"parent"`
	Kids        []int64 `json:"kids"`
	URL         string  `json:"url"`
	Score       int32   `json:"score"`
	Title       string  `json:"title"`
	Parts       []int64 `json:"parts"`
	Descendants int64   `json:"descendants"`
}

type hnProfile struct {
	ID        string  `json:"id"`
	Delay     int32   `json:"delay"`
	Created   int32   `json:"created"`
	Karma     int64   `json:"karma"`
	About     string  `json:"about"`
	Submitted []int64 `json:"submitted"`
}

const (
	updateURL  = "https://hacker-news.firebaseio.com/v0/updates.json?print=pretty"
	itemURL    = "https://hacker-news.firebaseio.com/v0/item/%d.json?print=pretty"
	profileURL = "https://hacker-news.firebaseio.com/v0/user/%s.json?print=pretty"
)

func hnUserToProtocol(p hnProfile) protocol.HnUser {
	var u protocol.HnUser
	u.Id = p.ID
	u.Created = p.Created
	u.Delay = p.Delay
	u.Karma = p.Karma
	u.About = p.About
	u.Submitted = p.Submitted
	return u
}

func hnPostToProtocol(p hnPost) protocol.HnPost {
	var o protocol.HnPost
	o.Id = p.ID
	o.Title = p.Title
	o.Author = p.By
	o.Deleted = p.Deleted
	o.Dead = p.Dead
	o.Descendants = p.Descendants
	o.Parent = p.Parent
	o.Kids = p.Kids
	o.Parts = p.Parts
	o.Time = p.Time
	o.Type = p.TypeStr
	o.Url = p.URL
	o.Text = p.Text
	o.Source = int32(protocol.HackerNews)
	o.Score = int64(p.Score)
	return o
}

func main() {

	clusterID := os.Getenv("NATS_CLUSTER_ID")
	clientID := os.Getenv("NATS_CLIENT_ID")
	if clientID == "" {
		clientID = "hacker-news-producer"
	}
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = stan.DefaultNatsURL
	}

	log.Infof("Connecting to nats server %s", natsURL)
	nc, err := stan.Connect(clusterID, clientID, stan.NatsURL(natsURL))
	if err != nil {
		log.Fatalf("Error connecting to nats-streaming server: %s", err)
	}
	log.Infof("Connected to nats-streaming. url = %s id = %s ", nc.NatsConn().ConnectedUrl(), nc.NatsConn().ConnectedServerId())
	for _, server := range nc.NatsConn().DiscoveredServers() {
		log.Infof("Discovered nats server %s", server)
	}

	defer nc.Close()

	client := &http.Client{}

	notifications := make(chan firego.Event)
	f := firego.New(updateURL, client)
	if err := f.Watch(notifications); err != nil {
		log.Fatal(err)
	}
	defer f.StopWatching()

	{
		getChan := make(chan *nats.Msg)
		nc.NatsConn().Subscribe(subjects.HackerNewsGetObject, func(m *nats.Msg) {
			getChan <- m
		})
		concurrency, err := strconv.Atoi(os.Getenv("HN_REQUEST_CONCURRENCY"))
		if err != nil {
			concurrency = 1
		}
		for i := 0; i < concurrency; i++ {
			go func(c chan *nats.Msg) {
				reqFgo := firego.New("", client)
				for {
					m := <-c
					start := time.Now()
					replySubject := m.Reply
					var request protocol.HnObjectRequest
					if err := proto.Unmarshal(m.Data, &request); err != nil {
						log.Fatal(err)
					}
					switch request.Type {
					case protocol.HnObjectRequest_USER:
						if replySubject == "" {
							replySubject = subjects.HackerNewsUsers
						}
						reqFgo.SetURL(fmt.Sprintf(profileURL, request.Username))
						var profile hnProfile
						err := reqFgo.Value(&profile)
						if err != nil {
							return
						}
						p := hnUserToProtocol(profile)
						replyBuffer, err := proto.Marshal(&p)
						if err != nil {
							log.Fatal(fmt.Sprintf("Error marshalling reply for %s", subjects.HackerNewsGetObject))
						}
						nc.NatsConn().Publish(replySubject, replyBuffer)
					case protocol.HnObjectRequest_POST:
						if replySubject == "" {
							replySubject = subjects.HackerNewsPosts
						}
						reqFgo.SetURL(fmt.Sprintf(itemURL, request.Id))
						var post hnPost
						err := reqFgo.Value(&post)
						if err != nil {
							return
						}
						p := hnPostToProtocol(post)
						replyBuffer, err := proto.Marshal(&p)
						if err != nil {
							log.Fatal(fmt.Sprintf("Error marshalling reply for %s", subjects.HackerNewsGetObject))
						}
						nc.NatsConn().Publish(replySubject, replyBuffer)
					}
					fmt.Printf("Response sent in %s for %s\n", time.Now().Sub(start).String(), request.Type.String())
				}
			}(getChan)
		}
	}

	fireGoClient := firego.New("", client)
	ackHandler := func(ackedNuid string, err error) {
		if err != nil {
			log.Errorf("Warning: error publishing msg id %s: %v\n", ackedNuid, err.Error())
		}
	}
	for event := range notifications {
		var items map[string][]interface{}
		if err := event.Value(&items); err != nil {
			log.Fatal(err)
		}
		for _, element := range items["items"] {
			fireGoClient.SetURL(fmt.Sprintf(itemURL, int(element.(float64))))
			var p hnPost
			if err := fireGoClient.Value(&p); err != nil {
				log.Fatal(err)
			}

			o := hnPostToProtocol(p)

			if bytes, err := proto.Marshal(&o); err != nil {
				log.Fatal(err)
			} else {
				nc.PublishAsync(subjects.HackerNewsPosts, bytes, ackHandler)
			}
		}
		for _, element := range items["profiles"] {
			fireGoClient.SetURL(fmt.Sprintf(profileURL, element))
			var p hnProfile
			if err := fireGoClient.Value(&p); err != nil {
				log.Fatal(err)
			}
			u := hnUserToProtocol(p)
			if bytes, err := proto.Marshal(&u); err != nil {
				log.Fatal(err)
			} else {
				nc.PublishAsync(subjects.HackerNewsUsers, bytes, ackHandler)
			}
		}
	}
}
