package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime/pprof"

	"github.com/ngaut/log"

	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/kabergstrom/site/db"
	"github.com/kabergstrom/site/protocol"
	"github.com/labstack/echo"
	mw "github.com/labstack/echo/middleware"
	"github.com/pkg/errors"
	"github.com/rainycape/memcache"
)

type apiCtx struct {
	mcObj     *memcache.Client
	mcListing *memcache.Client
}

type author struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

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
	Text    string `json:"text"`
	URL     string `json:"url"`
	Dead    bool   `json:"dead"`
	Author  author `json:"author"`
	NumKids int32  `json:"num_kids"`
}
type textPost struct {
	object
	Title   string `json:"title"`
	Text    string `json:"text"`
	Author  author `json:"author"`
	NumKids int32  `json:"num_kids"`
}
type comment struct {
	object
	Text    string `json:"text"`
	Author  author `json:"author"`
	Parent  string `json:"parent"`
	NumKids int32  `json:"num_kids"`
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

func getAuthor(obj db.Object) int64 {
	switch obj.Type {
	case protocol.LinkPost:
		fallthrough
	case protocol.Comment:
		fallthrough
	case protocol.TextPost:
		fallthrough
	case protocol.Job:
		fallthrough
	case protocol.Poll:
		fallthrough
	case protocol.PollOpt:
		return obj.Data.(*db.Post).Author
	}
	return 0
}

func dbObjectToAPIObject(obj db.Object, userMap map[int64]author) (retVal interface{}, err error) {
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
	switch obj.Type {
	case protocol.LinkPost:
		post := obj.Data.(*db.Post)
		retVal = linkPost{
			object:  o,
			Text:    post.Text,
			URL:     post.Url,
			Author:  userMap[post.Author],
			Dead:    post.Dead,
			NumKids: int32(len(obj.Kids.Kids)),
		}
		return
	case protocol.Comment:
		post := obj.Data.(*db.Post)
		retVal = comment{
			object:  o,
			Text:    post.Text,
			Author:  userMap[post.Author],
			Parent:  strconv.FormatInt(post.Parent, 10),
			NumKids: int32(len(obj.Kids.Kids)),
		}
		return
	case protocol.TextPost:
		post := obj.Data.(*db.Post)
		retVal = textPost{
			object:  o,
			Text:    post.Text,
			Title:   post.Title,
			Author:  userMap[post.Author],
			NumKids: int32(len(obj.Kids.Kids)),
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

func (api *apiCtx) hydrateAuthors(authors map[int64]author) error {
	authorsToGet := make([]string, len(authors))
	i := 0
	for val := range authors {
		authorsToGet[i] = strconv.FormatInt(val, 10)
		i++
	}
	/*items, err := api.mcObj.GetMulti(authorsToGet)
	if err != nil {
		return errors.Wrapf(err, "Failed to get authors")
	}*/
	items := make(map[string]*memcache.Item, len(authors))
	for id := range authors {
		key := strconv.FormatInt(id, 10)
		item, err := api.mcObj.Get(key)
		if err != nil {
			if err.Error() == "memcache: cache miss" {
				log.Infof("Author with id %s not in db", key)
				continue
			}
			return errors.Wrapf(err, "Failed to get item %s", key)
		}
		items[key] = item
	}
	for key, item := range items {
		obj, err := db.ParseMemCacheObj(item.Value)
		if err != nil {
			return errors.Wrapf(err, "Failed to parse object for id %s"+key)
		}
		user := obj.Data.(*db.User)
		authors[obj.ID] = author{Name: user.Name, ID: strconv.FormatInt(obj.ID, 10)}
	}
	return nil
}

func addEndpoints(e *echo.Echo, a *apiCtx) {
	e.GET("/exit", func(c echo.Context) error {
		e.Close()
		return nil
	})
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
		items, err := a.mcObj.GetMulti(req.IDs)
		if err != nil {
			log.Error(err)
			return c.String(http.StatusInternalServerError, fmt.Sprintf("Error getting ids %+v", req))
		}
		dbObjects := make([]db.Object, len(items))
		authors := make(map[int64]author, len(items))
		{
			i := 0
			for key, item := range items {
				obj, err := db.ParseMemCacheObj(item.Value)
				if err != nil {
					log.Error(err)
					return c.String(http.StatusInternalServerError, fmt.Sprintf("Error parsing object for id %s", key))
				}
				objAuthor := getAuthor(obj)
				if objAuthor != 0 {
					authors[objAuthor] = author{}
				}
				dbObjects[i] = obj
				i++
			}
			if err = a.hydrateAuthors(authors); err != nil {
				return err
			}
		}
		values := make([]interface{}, len(items))
		i := 0
		for _, item := range dbObjects {
			apiObj, err := dbObjectToAPIObject(item, authors)
			if err != nil {
				log.Error(err)
				return c.String(http.StatusInternalServerError, fmt.Sprintf("Error converting object for id %d", item.ID))
			}
			values[i] = apiObj
			i++
		}

		json, err := json.Marshal(values)
		if err != nil {
			log.Error(err)
			return c.String(http.StatusInternalServerError, fmt.Sprintf("Error marshalling items for ids %+v", req))
		}
		return c.String(http.StatusOK, string(json))
	})
	e.GET("/hot", func(c echo.Context) error {
		start := 0
		str := c.QueryParam("start")
		if str != "" {
			var err error
			start, err = strconv.Atoi(str)
			if err != nil {
				return c.String(http.StatusBadRequest, "Could not parse start parameter")
			}
		}
		end := start + 30
		str = c.QueryParam("count")
		if str != "" {
			count, err := strconv.Atoi(str)
			if err != nil {
				return c.String(http.StatusBadRequest, "Could not parse count parameter")
			}
			if count > 100 {
				return c.String(http.StatusBadRequest, "Max 100 items per request")
			}
			end = start + count
		}

		listingItem, err := a.mcListing.Get(strconv.Itoa(db.ListingHot))
		if err != nil {
			log.Error(err)
			return c.String(http.StatusInternalServerError, "Internal server error")
		}
		var listing db.Listing
		if err := proto.Unmarshal(listingItem.Value, &listing); err != nil {
			log.Error(err)
			return c.String(http.StatusInternalServerError, "Internal server error")
		}
		if len(listing.Objects) < start {
			end = start
		} else if len(listing.Objects) < end {
			end = len(listing.Objects) - 1
		}
		authors := make(map[int64]author, end-start)
		dbObjects := make([]db.Object, end-start)
		for i, val := range listing.Objects[start:end] {
			item, err := a.mcObj.Get(strconv.FormatInt(val, 10))
			if err != nil {
				log.Error(err)
				return c.String(http.StatusNotFound, fmt.Sprintf("Could not find %d", val))
			}
			obj, err := db.ParseMemCacheObj(item.Value)
			if err != nil {
				log.Error(err)
				return c.String(http.StatusInternalServerError, fmt.Sprintf("Error parsing object with id %d", val))
			}
			objAuthor := getAuthor(obj)
			if objAuthor != 0 {
				authors[objAuthor] = author{}
			}

			dbObjects[i] = obj
		}
		if err = a.hydrateAuthors(authors); err != nil {
			log.Error(err)
			return c.String(http.StatusInternalServerError, "Internal server error")
		}

		var values []interface{}
		for _, obj := range dbObjects {
			apiObj, err := dbObjectToAPIObject(obj, authors)
			if err != nil {
				log.Error(err)
				return c.String(http.StatusInternalServerError, fmt.Sprintf("Error converting object for id %d", obj.ID))
			}
			values = append(values, apiObj)
		}

		if c.QueryParam("pretty") != "" {
			return c.JSONPretty(200, values, "  ")
		}
		return c.JSON(200, values)
	})
	e.GET("/object/:id", func(c echo.Context) error {
		str := c.Param("id")
		_, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return c.String(http.StatusBadRequest, "Invalid id")
		}
		item, err := a.mcObj.Get(str)
		if err != nil {
			log.Error(err)
			return c.String(http.StatusNotFound, fmt.Sprintf("Could not find %s", str))
		}
		obj, err := db.ParseMemCacheObj(item.Value)
		if err != nil {
			log.Error(err)
			return c.String(http.StatusInternalServerError, fmt.Sprintf("Error parsing id %s", str))
		}
		authors := make(map[int64]author, 1)
		objAuthor := getAuthor(obj)
		if objAuthor != 0 {
			authors[objAuthor] = author{}
		}
		if err = a.hydrateAuthors(authors); err != nil {
			log.Error(err)
			return c.String(http.StatusInternalServerError, fmt.Sprintf("Internal server error"))
		}
		apiObj, err := dbObjectToAPIObject(obj, authors)
		if err != nil {
			log.Error(err)
			return c.String(http.StatusInternalServerError, fmt.Sprintf("Error converting object for id %s", str))
		}
		if c.QueryParam("pretty") != "" {
			return c.JSONPretty(200, apiObj, "  ")
		}
		return c.JSON(200, apiObj)
	})
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func openMemcachedView(url string, viewName string) (*memcache.Client, error) {
	mc, err := memcache.New(url)
	if err != nil {
		return nil, err
	}
	if _, err := mc.Get("@@" + viewName); err != nil {
		return nil, err
	}
	return mc, err
}

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		log.Error("Starting cpuprofile")
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	memcacheAddr := os.Getenv("API_MEMCACHE_ADDRESS")
	mcObj, err := openMemcachedView(memcacheAddr, "object_data")
	if err != nil {
		log.Fatalf("Failed to connect to MySQL memcache plugin on %s : %s", memcacheAddr, err)
	}
	mcListing, err := openMemcachedView(memcacheAddr, "listing_data")
	if err != nil {
		log.Fatalf("Failed to connect to MySQL memcache plugin on %s : %s", memcacheAddr, err)
	}

	api := apiCtx{
		mcObj:     mcObj,
		mcListing: mcListing,
	}

	e := echo.New()

	e.Use(mw.Logger())
	e.Use(mw.Recover())
	addEndpoints(e, &api)
	serverHost := os.Getenv("API_SERVER_HOST")
	serverPort := os.Getenv("API_SERVER_PORT")
	e.Logger.Fatal(e.Start(fmt.Sprintf("%s:%s", serverHost, serverPort)))
}
