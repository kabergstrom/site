package main

import (
	"log"
	"time"

	"strconv"

	"sort"

	"github.com/pkg/errors"

	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/kabergstrom/site/db"
	"github.com/kabergstrom/site/protocol"
	"github.com/kabergstrom/site/protocol/subjects"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/rainycape/memcache"
)

type rankingCtx struct {
	stan      stan.Conn
	mcObj     *memcache.Client
	mcListing *memcache.Client
}

type HotSort []db.Object

func (a HotSort) Len() int           { return len(a) }
func (a HotSort) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a HotSort) Less(i, j int) bool { return a[i].Score+a[i].SourceScore > a[j].Score+a[j].SourceScore }

func (r *rankingCtx) updateCaches(objectsChanged []int64, listingType int, sortFunc func(values []db.Object) sort.Interface) error {

	listingItem, err := r.mcListing.Get(strconv.Itoa(listingType))
	var listing db.Listing
	if err != nil {
		if err.Error() != "memcache: cache miss" {
			return errors.Wrapf(err, "Error getting listing with id %d", listingType)
		}
		err = nil
	} else {
		err = proto.Unmarshal(listingItem.Value, &listing)
		if err != nil {
			return errors.Wrapf(err, "Error unmarshaling listing of type %d", listingType)
		}
	}
	allObjects := make(map[int64]bool, len(listing.Objects)+len(objectsChanged))
	for _, val := range listing.Objects {
		allObjects[val] = true
	}
	for _, val := range objectsChanged {
		allObjects[val] = true
	}
	dbObjects := make([]db.Object, len(allObjects))
	i := len(allObjects)
	for val := range allObjects {
		i--
		item, err := r.mcObj.Get(strconv.FormatInt(val, 10))
		if err != nil {
			if err.Error() == "memcache: cache miss" {
				dbObjects = append(dbObjects[:i],
					dbObjects[i+1:]...)
				continue
			}
			return errors.Wrapf(err, "Error getting object with id %d", val)
		}
		obj, err := db.ParseMemCacheObj(item.Value)
		if err != nil {
			return errors.Wrapf(err, "Error parsing object with id %d", val)
		}
		switch obj.Type {
		case protocol.LinkPost:
		case protocol.TextPost:
		case protocol.Job:
		case protocol.Poll:
			break
		default:
			dbObjects = append(dbObjects[:i],
				dbObjects[i+1:]...)
			continue
		}
		dbObjects[i] = obj
	}
	sort.Sort(sortFunc(dbObjects))
	listingSize := len(dbObjects)
	if listingSize > db.MaxListingSize {
		listingSize = db.MaxListingSize
	}
	var newListing db.Listing
	newListing.Objects = make([]int64, listingSize)
	for i, val := range dbObjects[:listingSize] {
		newListing.Objects[i] = val.ID
	}
	bytes, err := proto.Marshal(&newListing)
	if err != nil {
		return errors.Wrapf(err, "Error serializing lising %d", listingType)
	}
	err = r.mcListing.Set(&memcache.Item{
		Key:   strconv.Itoa(listingType),
		Value: bytes,
	})
	if err != nil {
		return errors.Wrapf(err, "Error setting lising %d", listingType)
	}
	return nil
}

func openMemcachedView(url string, viewName string) (*memcache.Client, error) {
	mc, err := memcache.New(url)
	if err != nil {
		return nil, err
	}
	mc.Get("@@" + viewName)
	return mc, err
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	mcObj, err := openMemcachedView("localhost:11211", "object_data")
	if err != nil {
		log.Fatal(err)
	}
	mcListing, err := openMemcachedView("localhost:11211", "listing_data")
	if err != nil {
		log.Fatal(err)
	}

	clusterID := "test-cluster"
	nc, _ := stan.Connect(clusterID, "ranking")
	defer nc.Close()
	objModChannel := make(chan *stan.Msg)

	ranking := rankingCtx{
		stan:      nc,
		mcObj:     mcObj,
		mcListing: mcListing,
	}

	aw := time.Second * 30
	maxInFlight := 4096
	nc.Subscribe(subjects.ObjectsModified, func(m *stan.Msg) { objModChannel <- m }, stan.SetManualAckMode(), stan.AckWait(aw), stan.DurableName("ranking"), stan.StartAt(pb.StartPosition_First), stan.MaxInflight(maxInFlight))

	type modMsg struct {
		msg         *stan.Msg
		objModified int64
	}
	ticker := time.NewTimer(time.Second * 5)
	var windowBuffer []modMsg
	for {
		select {
		case m := <-objModChannel:
			var mod protocol.ObjectModified
			if err := proto.Unmarshal(m.Data, &mod); err != nil {
				log.Fatal(err)
			}
			windowBuffer = append(windowBuffer, modMsg{msg: m, objModified: mod.Id})
			if len(windowBuffer) == maxInFlight {
				ticker.Reset(0)
			}
		case <-ticker.C:
			start := time.Now()
			objectsChanged := make([]int64, len(windowBuffer))
			for i, val := range windowBuffer {
				objectsChanged[i] = val.objModified
			}
			err := ranking.updateCaches(objectsChanged, db.ListingHot, func(data []db.Object) sort.Interface { return HotSort(data) })
			if err != nil {
				log.Fatal(err)
			}
			for _, val := range windowBuffer {
				val.msg.Ack()
			}
			fmt.Printf("Sorted ranking for %d objects in %s\n", len(objectsChanged), time.Now().Sub(start).String())
			windowBuffer = windowBuffer[:0]
			ticker.Reset(time.Second * 5)
		}
	}
}
