package main

import (
	"time"

	"fmt"

	"database/sql"

	"log"

	"strconv"

	"github.com/PuerkitoBio/purell"
	"github.com/bwmarrin/snowflake"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/gogo/protobuf/proto"
	"github.com/kabergstrom/site/db"
	"github.com/kabergstrom/site/protocol"
	"github.com/kabergstrom/site/protocol/subjects"
	nats "github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
)

type postProcessor struct {
	db        *db.Database
	snowflake *snowflake.Node
	stan      stan.Conn
}
type processingContext struct {
	processedUsers map[string]int64
	processedPosts map[int64]int64
}

func typeToTypeID(source protocol.SourceID, typeStr string) protocol.ObjectType {
	if source == protocol.HackerNews {
		if typeStr == "job" {
			return protocol.Job
		} else if typeStr == "story" {
			return protocol.TextPost
		} else if typeStr == "comment" {
			return protocol.Comment
		} else if typeStr == "poll" {
			return protocol.Poll
		} else if typeStr == "pollopt" {
			return protocol.PollOpt
		}
		log.Fatal(fmt.Sprintf("Unrecognized type %s for sourceID %d", typeStr, source))
	}
	return 0
}

func hnUserIDtoDatabaseID(id string) []byte {
	return []byte("u" + id)
}
func hnPostIDtoDatabaseID(id int64) []byte {
	return []byte("p" + string(strconv.FormatInt(id, 10)))
}

func hnUserToDBObject(user protocol.HnUser, id int64, submitted []int64) (obj db.Object, err error) {
	obj.ID = id
	obj.Source = protocol.HackerNews
	obj.Type = protocol.User
	obj.Score = 0
	obj.SourceScore = user.Karma
	obj.Deleted = false
	obj.UnixTime = user.Created
	obj.Compression = db.None
	obj.Encoding = db.Protobuf
	var serializedUser db.User
	serializedUser.Name = user.Id
	serializedUser.About = user.About
	obj.Data = &serializedUser
	obj.Kids = db.Kids{Kids: submitted}
	return
}

func (proc *postProcessor) processHnPost(m *stan.Msg) {
	fmt.Printf("Processing post %s\n", strconv.FormatInt(int64(m.Sequence), 10))
	var ctx processingContext
	ctx.processedPosts = make(map[int64]int64)
	ctx.processedUsers = make(map[string]int64)
	for {
		if err := proc.onHackerNewsPost(m.Data, ctx); err == nil {
			break
		} else {
			fmt.Printf("Error processing post %s: %s", strconv.FormatInt(int64(m.Sequence), 10), err)
		}
	}
	m.Ack()
	fmt.Printf("Acked post %s\n", strconv.FormatInt(int64(m.Sequence), 10))
}

func (proc *postProcessor) getPostIDFromHNID(hnID int64, ctx processingContext) (int64, error) {
	timeout, _ := time.ParseDuration("1s")
	if val, ok := ctx.processedPosts[hnID]; ok {
		return val, nil
	}
	dbPartID, err := proc.db.GetObjectIDFromSourceID(protocol.HackerNews, hnPostIDtoDatabaseID(hnID))
	if err != nil {
		request := protocol.HnObjectRequest{
			Id:   hnID,
			Type: protocol.HnObjectRequest_POST,
		}
		payload, err := proto.Marshal(&request)
		if err != nil {
			return 0, err
		}
		requestStart := time.Now()
		msg, err := proc.stan.NatsConn().Request(subjects.HackerNewsGetObject, payload, timeout)
		if err != nil {
			return 0, err
		}
		fmt.Printf("Request for post %s took %s\n", strconv.FormatInt(hnID, 10), time.Now().Sub(requestStart).String())

		proc.onHackerNewsPost(msg.Data, ctx)
		if val, ok := ctx.processedPosts[hnID]; ok {
			return val, nil
		}
		dbPartID, err = proc.db.GetObjectIDFromSourceID(protocol.HackerNews, hnPostIDtoDatabaseID(hnID))
		if err != nil {
			return 0, err
		}
		return dbPartID, nil
	}
	return dbPartID, nil
}

func (proc *postProcessor) onHackerNewsPost(postData []byte, ctx processingContext) error {

	var p protocol.HnPost
	if err := proto.Unmarshal(postData, &p); err != nil {
		log.Fatal(err)
	}
	if _, ok := ctx.processedPosts[p.Id]; ok {
		return nil
	}

	objectID, err := proc.db.GetObjectIDFromSourceID(protocol.SourceID(p.Source), hnPostIDtoDatabaseID(p.Id))
	if err != nil {
		objectID = proc.snowflake.Generate().Int64()
		proc.db.InsertSourceIDToObjectID(objectID, protocol.SourceID(p.Source), hnPostIDtoDatabaseID(p.Id))
	}
	ctx.processedPosts[p.Id] = objectID
	if p.Type == "title" {
		fmt.Printf("Processed post with title %s\n", p.Title)
	}
	timeout, _ := time.ParseDuration("1s")

	var dbData db.Post
	if p.Author != "" {
		userID, ok := ctx.processedUsers[p.Author]
		if ok == false {
			userID, err = proc.db.GetObjectIDFromSourceID(protocol.SourceID(p.Source), hnUserIDtoDatabaseID(p.Author))
			if err != nil {
				userID = proc.snowflake.Generate().Int64()
				if err := proc.db.InsertSourceIDToObjectID(userID, protocol.SourceID(p.Source), hnUserIDtoDatabaseID(p.Author)); err != nil {
					log.Fatal(err)
				}
				ctx.processedUsers[p.Author] = userID
				request := protocol.HnObjectRequest{
					Username: p.Author,
					Type:     protocol.HnObjectRequest_USER,
				}
				payload, err := proto.Marshal(&request)
				if err != nil {
					log.Fatal(err)
				}
				requestStart := time.Now()
				msg, err := proc.stan.NatsConn().Request(subjects.HackerNewsGetObject, payload, timeout)
				if err != nil {
					return err
				}
				fmt.Printf("Request for user %s took %s\n", p.Author, time.Now().Sub(requestStart).String())
				var user protocol.HnUser
				if err := proto.Unmarshal(msg.Data, &user); err != nil {
					log.Fatal(err)
				}
				var submittedIDs []int64
				/*for _, submitted := range user.Submitted {
					dbID, err := proc.getPostIDFromHNID(submitted, ctx)
					if err != nil {
						log.Fatal(err)
					}
					submittedIDs = append(submittedIDs, dbID)
				}*/
				dbObj, err := hnUserToDBObject(user, userID, submittedIDs)
				if err != nil {
					log.Fatal(err)
				}
				err = proc.db.InsertObject(dbObj)
				if err != nil {
					log.Fatal(err)
				}
				userID = dbObj.ID
			}
		}
		dbData.Author = userID
	}

	dbData.Dead = p.Dead
	dbData.Parent = p.Parent
	if p.Parent != 0 {
		parentID, err := proc.getPostIDFromHNID(p.Parent, ctx)
		if err != nil {
			return err
		}
		dbData.Parent = parentID
	}
	url, err := purell.NormalizeURLString(p.Url, purell.FlagLowercaseScheme|purell.FlagLowercaseHost|purell.FlagUppercaseEscapes)
	if err != nil {
		log.Fatal(err)
	}
	dbData.Url = url
	dbData.Title = p.Title
	dbData.Text = p.Text
	if p.Parts != nil && len(p.Parts) > 0 {
		for _, hnPartID := range p.Parts {
			dbPartID, err := proc.getPostIDFromHNID(hnPartID, ctx)
			if err != nil {
				return err
			}
			dbData.Parts = append(dbData.Parts, dbPartID)
		}
	}

	var obj db.Object
	obj.ID = objectID
	obj.Source = protocol.HackerNews
	switch p.Type {
	case "job":
		obj.Type = protocol.Job
	case "story":
		obj.Type = protocol.TextPost
	case "comment":
		obj.Type = protocol.Comment
	case "poll":
		obj.Type = protocol.Poll
	case "pollopt":
		obj.Type = protocol.PollOpt
	}
	obj.SourceScore = p.Score
	obj.Deleted = p.Deleted
	obj.UnixTime = int32(p.Time)
	obj.Compression = db.None
	obj.Encoding = db.Protobuf
	obj.Data = &dbData

	var commentIDs []int64
	for _, hnCommentID := range p.Kids {
		dbID, err := proc.getPostIDFromHNID(hnCommentID, ctx)
		if err != nil {
			return err
		}
		commentIDs = append(commentIDs, dbID)
	}
	obj.Kids = db.Kids{Kids: commentIDs}
	if err := proc.db.InsertObject(obj); err != nil {
		me, ok := err.(*mysql.MySQLError)
		if !ok {
			log.Fatal(err)
		}
		// if it's not duplicate key error, bail
		if me.Number != 1062 {
			log.Fatal(err)
		}
		existingObj, version, err := proc.db.GetObject(objectID)
		if err != nil {
			log.Fatal(err)
		}
		existingObj.Deleted = obj.Deleted
		existingObj.SourceScore = obj.SourceScore
		existingObj.Compression = obj.Compression
		existingObj.Encoding = obj.Encoding
		existingObj.Data = obj.Data
		// Join existing and new kids array to accomodate comments from different sources
		{
			joinedKidsMap := make(map[int64]bool)
			for _, comment := range commentIDs {
				joinedKidsMap[comment] = true
			}
			kidsArray := existingObj.Kids.Kids
			for _, kid := range kidsArray {
				joinedKidsMap[kid] = true
			}
			joinedKids := make([]int64, len(joinedKidsMap))
			i := 0
			for k := range joinedKidsMap {
				joinedKids[i] = k
				i++
			}

			existingObj.Kids = db.Kids{Kids: joinedKids}
			existingObj.NumKids = int32(len(existingObj.Kids.Kids))
		}
		err = proc.db.UpdateSourceObject(obj, version)
		if err != nil {
			log.Fatal(err)
		}
	}
	return nil
}

func main() {

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var processor postProcessor
	snowflake, err := snowflake.NewNode(1)
	if err != nil {
		fmt.Println(err)
		return
	}
	processor.snowflake = snowflake
	clusterID := "test-cluster"
	nc, err := stan.Connect(clusterID, "nats2db")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	processor.stan = nc

	sql, err := sql.Open("mysql", "root:test@tcp(54.149.61.225:3306)/site")
	if err != nil {
		log.Fatal(err)
	}
	defer sql.Close()
	dbi, err := db.NewDBI(sql)
	if err != nil {
		log.Fatal(err)
	}
	processor.db = dbi

	aw, _ := time.ParseDuration("1s")

	hnPostChannel := make(chan *stan.Msg)

	nc.Subscribe(subjects.HackerNewsPosts, func(m *stan.Msg) { hnPostChannel <- m }, stan.SetManualAckMode(), stan.AckWait(aw), stan.DurableName("nats2db"), stan.StartAt(pb.StartPosition_First))
	go func() {
		for {
			msg := <-hnPostChannel
			processor.processHnPost(msg)
		}
	}()
	for nc.NatsConn().Status() != nats.DISCONNECTED {
		time.Sleep(time.Second)
	}
}
