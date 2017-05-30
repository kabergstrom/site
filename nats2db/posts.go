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

	"github.com/pkg/errors"
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
	if err := proc.onHackerNewsPost(m.Data, ctx); err == nil {
		m.Ack()
		fmt.Printf("Acked post %s\n", strconv.FormatInt(int64(m.Sequence), 10))
	} else {
		fmt.Printf("Error processing post %s: %s", strconv.FormatInt(int64(m.Sequence), 10), err)
	}
}

func (proc *postProcessor) getUserIDFromHNID(author string, ctx processingContext) (userID int64, err error) {
	dbAuthor := hnUserIDtoDatabaseID(author)
	source := protocol.HackerNews
	userID, ok := ctx.processedUsers[author]
	if ok == false {
		userID, err = proc.db.GetObjectIDFromSourceID(source, dbAuthor)
		if err != nil {
			err = nil
			userID = proc.snowflake.Generate().Int64()
			if err := proc.db.InsertSourceIDToObjectID(userID, source, dbAuthor); err != nil {
				me, ok := err.(*mysql.MySQLError)
				if !ok {
					log.Fatal(err)
				}
				// if it's not duplicate key error, bail
				if me.Number != 1062 {
					log.Fatal(err)
				}
				userID, err = proc.db.GetObjectIDFromSourceID(source, dbAuthor)
				if err != nil {
					log.Fatal(err)
				}
			}
			ctx.processedUsers[author] = userID
			request := protocol.HnObjectRequest{
				Username: author,
				Type:     protocol.HnObjectRequest_USER,
			}
			payload, err := proto.Marshal(&request)
			if err != nil {
				log.Fatal(err)
			}
			requestStart := time.Now()
			timeout, _ := time.ParseDuration("10s")
			msg, err := proc.stan.NatsConn().Request(subjects.HackerNewsGetObject, payload, timeout)
			if err != nil {
				return 0, err
			}
			fmt.Printf("Request for user %s took %s\n", author, time.Now().Sub(requestStart).String())
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
				me, ok := err.(*mysql.MySQLError)
				if !ok {
					log.Fatal(err)
				}
				// if it's not duplicate key error, bail
				if me.Number != 1062 {
					log.Fatal(err)
				}
				err = nil // otherwise, ignore error
			}
			userID = dbObj.ID
		}
	}
	return
}

func (proc *postProcessor) getPostIDFromHNID(hnID int64, ctx processingContext) (int64, error) {
	timeout, _ := time.ParseDuration("10s")
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

	var dbData db.Post
	if p.Author != "" {
		userID, err := proc.getUserIDFromHNID(p.Author, ctx)
		if err != nil {
			return errors.Wrapf(err, "Error getting author from HN name %s\n", p.Author)
		}
		dbData.Author = userID
	}

	dbData.Dead = p.Dead
	dbData.Parent = p.Parent
	if p.Parent != 0 {
		parentID, err := proc.getPostIDFromHNID(p.Parent, ctx)
		if err != nil {
			return errors.Wrapf(err, "Error getting post from HN id %d\n", p.Parent)
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
				return errors.Wrapf(err, "Error getting post part from HN id %d\n", hnPartID)
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
	default:
		log.Fatal(fmt.Sprintf("Unrecognized hackernews object type %s", p.Type))
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
			return errors.Wrapf(err, "Error getting comment from HN id %d\n", hnCommentID)
		}
		commentIDs = append(commentIDs, dbID)
	}
	obj.Kids = db.Kids{Kids: commentIDs}
	obj.NumKids = int32(len(commentIDs))
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
		mod := protocol.ObjectModified{
			Id: objectID,
		}
		payload, err := proto.Marshal(&mod)
		if err != nil {
			log.Fatal(err)
		}
		proc.stan.PublishAsync(subjects.ObjectsModified, payload, nil)
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

	sql, err := sql.Open("mysql", "root:test@tcp(localhost:3306)/site")
	if err != nil {
		log.Fatal(err)
	}
	defer sql.Close()
	dbi, err := db.NewDBI(sql)
	if err != nil {
		log.Fatal(err)
	}
	processor.db = dbi

	aw, _ := time.ParseDuration("30s")

	hnPostChannel := make(chan *stan.Msg)

	nc.Subscribe(subjects.HackerNewsPosts, func(m *stan.Msg) { hnPostChannel <- m }, stan.SetManualAckMode(), stan.AckWait(aw), stan.DurableName("nats2db"), stan.StartAt(pb.StartPosition_First))
	concurrency := 10
	for i := 0; i < concurrency; i++ {
		go func(c chan *stan.Msg) {
			for {
				msg := <-c
				processor.processHnPost(msg)
			}
		}(hnPostChannel)
	}
	for nc.NatsConn().Status() != nats.DISCONNECTED {
		time.Sleep(time.Second)
	}
}
