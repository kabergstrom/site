package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"strconv"

	"io"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/kabergstrom/site/db"
	"github.com/kabergstrom/site/protocol"
	"github.com/labstack/echo"
	"github.com/rainycape/memcache"
)

type object struct {
	ID       string `json:"id"`
	Source   string `json:"source"`
	Type     string `json:"type"`
	Score    int64  `json:"score"`
	Deleted  bool   `json:"deleted"`
	UnixTime int32  `json:"created"`
}

type user struct {
	object
	Name  string `json:"name"`
	About string `json:"about"`
}
type linkPost struct {
	object
	Text   string   `json:"text"`
	URL    string   `json:"url"`
	Dead   bool     `json:"dead"`
	Author string   `json:"author"`
	Kids   []string `json:"kids"`
}
type textPost struct {
	object
	Title  string   `json:"title"`
	Text   string   `json:"text"`
	Author string   `json:"author"`
	Kids   []string `json:"kids"`
}
type comment struct {
	object
	Text   string   `json:"text"`
	Author string   `json:"author"`
	Parent string   `json:"parent"`
	Kids   []string `json:"kids"`
}

func parseMemCacheObj(val []byte) (obj db.Object, err error) {
	str := string(val)
	endIndex := 0
	for i := 0; i < 10; i++ {
		increment := strings.Index(str[endIndex+1:], "|")
		if increment == -1 {
			err = io.EOF
			return
		}
		endIndex += increment + 1
	}
	deletedNum := 0
	wotStr := string(val[:endIndex])
	_, err = fmt.Sscanf(wotStr, "%d|%d|%d|%d|%d|%d|%d|%d|%d|%d", &obj.ID, &obj.Source, &obj.Type, &obj.Score, &obj.SourceScore, &deletedNum, &obj.UnixTime, &obj.Compression, &obj.Encoding, &obj.NumKids)
	if err != nil {
		return
	}
	data, err := db.DecodeData(val[endIndex+1:], obj.Type)
	if err != nil {
		return
	}
	obj.Data = data

	serializedSize, err := proto.NewBuffer(val[endIndex+1:]).DecodeVarint()
	if err != nil {
		return
	}
	newStart := endIndex + int(serializedSize) + 2 + proto.SizeVarint(uint64(serializedSize))
	kids, err := db.DecodeKids(val[newStart:])
	fmt.Println("values = " + string(val[:endIndex]))
	fmt.Println("data = " + string(val[endIndex+1:]))
	fmt.Println("kids = " + string(val[newStart:]))
	if err != nil {
		return
	}
	obj.Kids = kids
	return
}
func apiObjectType(t protocol.ObjectType) (string, error) {
	switch t {
	case protocol.LinkPost:
		return "link", nil
	case protocol.Comment:
		return "comment", nil
	case protocol.TextPost:
		return "story", nil
	case protocol.Job:
		return "job", nil
	case protocol.Poll:
		return "poll", nil
	case protocol.PollOpt:
		return "pollopt", nil
	case protocol.User:
		return "user", nil
	default:
		return "", fmt.Errorf("Unhandled protocol.ObjectType %d", t)
	}
}

func dbObjectToAPIObject(obj db.Object) (retVal interface{}, err error) {
	var o object
	o.ID = strconv.FormatInt(obj.ID, 10)
	typeStr, err := apiObjectType(obj.Type)
	if err != nil {
		return
	}
	o.Type = typeStr
	o.Score = obj.Score + obj.SourceScore
	o.Deleted = obj.Deleted
	o.UnixTime = obj.UnixTime
	var kids []string
	if len(obj.Kids.Kids) > 0 {
		kids = make([]string, len(obj.Kids.Kids))
		for idx, kid := range obj.Kids.Kids {
			kids[idx] = strconv.FormatInt(kid, 10)
		}
	}
	switch obj.Type {
	case protocol.LinkPost:
		post := obj.Data.(*db.Post)
		retVal = linkPost{
			object: o,
			Text:   post.Text,
			URL:    post.Url,
			Author: strconv.FormatInt(post.Author, 10),
			Dead:   post.Dead,
			Kids:   kids,
		}
		return
	case protocol.Comment:
		post := obj.Data.(*db.Post)
		retVal = comment{
			object: o,
			Text:   post.Text,
			Author: strconv.FormatInt(post.Author, 10),
			Parent: strconv.FormatInt(post.Parent, 10),
			Kids:   kids,
		}
		return
	case protocol.TextPost:
		post := obj.Data.(*db.Post)
		retVal = textPost{
			object: o,
			Text:   post.Text,
			Title:  post.Title,
			Author: strconv.FormatInt(post.Author, 10),
			Kids:   kids,
		}
		return
	case protocol.User:
		u := obj.Data.(*db.User)
		retVal = user{
			object: o,
			Name:   u.Name,
			About:  u.About,
		}
		return
	default:
		err = fmt.Errorf("Unhandled protocol.ObjectType %d", obj.Type)
		return
	}
}

func main() {

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	mc, err := memcache.New("localhost:11211")
	if err != nil {
		log.Fatal(err)
	}
	mc.Get("@@object_data")

	e := echo.New()
	e.POST("/object/bulk", func(c echo.Context) error {
		type bulkObjectRequest struct {
			IDs []string `json:"ids"`
		}
		var req bulkObjectRequest
		err := json.NewDecoder(c.Request().Body).Decode(&req)
		if err != nil {
			return c.String(http.StatusBadRequest, "Invalid json")
		}
		if len(req.IDs) == 0 {
			return c.String(http.StatusBadRequest, "No ids supplied")
		}
		for _, id := range req.IDs {
			if _, err := strconv.ParseInt(id, 10, 64); err != nil {
				return c.String(http.StatusBadRequest, fmt.Sprintf("Invalid id %s", id))
			}
		}
		items, err := mc.GetMulti(req.IDs)
		if err != nil {
			log.Println(err)
			return c.String(http.StatusInternalServerError, fmt.Sprintf("Error getting ids %+v", req))
		}
		values := make([]interface{}, len(items))
		i := 0
		for key, item := range items {
			obj, err := parseMemCacheObj(item.Value)
			if err != nil {
				log.Println(err)
				return c.String(http.StatusInternalServerError, fmt.Sprintf("Error parsing object for id %s", key))
			}
			apiObj, err := dbObjectToAPIObject(obj)
			if err != nil {
				log.Println(err)
				return c.String(http.StatusInternalServerError, fmt.Sprintf("Error converting object for id %s", key))
			}
			values[i] = apiObj
			i++
		}

		json, err := json.Marshal(values)
		if err != nil {
			log.Println(err)
			return c.String(http.StatusInternalServerError, fmt.Sprintf("Error marshalling items for ids %+v", req))
		}
		return c.String(http.StatusOK, string(json))
	})
	e.GET("/object/:id", func(c echo.Context) error {
		str := c.Param("id")
		_, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return c.String(http.StatusBadRequest, "Invalid id")
		}
		item, err := mc.Get(str)
		if err != nil {
			log.Println(err)
			return c.String(http.StatusNotFound, fmt.Sprintf("Could not find %s", str))
		}
		obj, err := parseMemCacheObj(item.Value)
		if err != nil {
			log.Println(err)
			return c.String(http.StatusInternalServerError, fmt.Sprintf("Error parsing id %s", str))
		}
		apiObj, err := dbObjectToAPIObject(obj)
		if err != nil {
			log.Println(err)
			return c.String(http.StatusInternalServerError, fmt.Sprintf("Error converting object for id %s", str))
		}
		json, err := json.Marshal(apiObj)
		if err != nil {
			log.Println(err)
			return c.String(http.StatusInternalServerError, fmt.Sprintf("Error parsing id %s", str))
		}
		return c.String(http.StatusOK, string(json))
	})
	e.Logger.Fatal(e.Start(":1323"))
}
