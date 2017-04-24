package main

import (
	"log"
	"strconv"

	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/kabergstrom/site/protocol"
	"github.com/kabergstrom/site/protocol/subjects"
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
	Delay     int64   `json:"delay"`
	Created   int64   `json:"created"`
	Karma     int64   `json:"karma"`
	About     string  `json:"about"`
	Submitted []int64 `json:"submitted"`
}

func main() {
	clusterID := "test-cluster"
	nc, _ := stan.Connect(clusterID, "hacker-news-producer")

	defer nc.Close()

	client := &http.Client{}

	notifications := make(chan firego.Event)
	f := firego.New("https://hacker-news.firebaseio.com/v0/updates.json?print=pretty", client)
	if err := f.Watch(notifications); err != nil {
		log.Fatal(err)
	}
	defer f.StopWatching()
	fireGoClient := firego.New("", client)
	for event := range notifications {
		var items map[string][]interface{}
		if err := event.Value(&items); err != nil {
			log.Fatal(err)
		}
		for _, element := range items["items"] {
			fireGoClient.SetURL("https://hacker-news.firebaseio.com/v0/item/" + strconv.Itoa(int(element.(float64))) + ".json?print=pretty")
			var p hnPost
			if err := fireGoClient.Value(&p); err != nil {
				log.Fatal(err)
			}

			var o protocol.Post
			o.Id = p.ID
			o.Title = p.Title
			o.Author = p.By
			o.Deleted = p.Deleted
			o.Dead = p.Dead
			o.Descendants = p.Descendants
			o.Parent = p.Parent
			o.Kids = p.Kids
			o.Parts = p.Parts
			o.Time = int64(p.Time * 1000)
			o.Type = p.TypeStr
			o.Url = p.URL
			o.Text = p.Text
			ackHandler := func(ackedNuid string, err error) {
				if err != nil {
					log.Printf("Warning: error publishing msg id %s: %v\n", ackedNuid, err.Error())
				}
			}

			if bytes, err := proto.Marshal(&o); err != nil {
				log.Fatal(err)
			} else {
				nc.PublishAsync(subjects.Posts, bytes, ackHandler)
			}
		}
		/*for _, element := range items["profiles"] {
			fireGoClient.SetURL("https://hacker-news.firebaseio.com/v0/user/" + element.(string) + ".json?print=pretty")
			var p hnProfile
			if err := fireGoClient.Value(&p); err != nil {
				log.Fatal(err)
			}
		}*/
	}
}
