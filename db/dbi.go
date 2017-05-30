package db

import (
	"database/sql"
	"fmt"
	"io"
	"strings"

	"reflect"

	"github.com/gogo/protobuf/proto"
	"github.com/kabergstrom/site/protocol"
)

const (
	// ListingHot ID for the cache of the "hot" listing
	ListingHot = 1

	// MaxListingSize the max size of a listing
	MaxListingSize = 800
)

// Database interface for structured db updates
type Database struct {
	insertObjectStmt            *sql.Stmt
	updateSourceObject          *sql.Stmt
	insertURL                   *sql.Stmt
	insertSourceIDToObjectID    *sql.Stmt
	getObject                   *sql.Stmt
	getObjectIDFromSourceIDStmt *sql.Stmt
	db                          *sql.DB
}

// ParseMemCacheObj parses the object_data innodb memcached view into a db.Object
func ParseMemCacheObj(val []byte) (obj Object, err error) {
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
	data, err := DecodeData(val[endIndex+1:], obj.Type)
	if err != nil {
		return
	}
	obj.Data = data

	serializedSize, err := proto.NewBuffer(val[endIndex+1:]).DecodeVarint()
	if err != nil {
		return
	}
	newStart := endIndex + int(serializedSize) + 2 + proto.SizeVarint(uint64(serializedSize))
	kids, err := DecodeKids(val[newStart:])
	if err != nil {
		return
	}
	obj.Kids = kids
	return
}

// NewDBI initialize a new database. Prepares statements
func NewDBI(db *sql.DB) (retVal *Database, err error) {
	var i Database
	insertObjectStmt, err := db.Prepare("INSERT INTO object (id, source, type, score, source_score, deleted, unixtime, compression, encoding, data, kids, num_kids) VALUES (?, ?, ? ,? ,? , ?, ?, ? ,?, ?, ?, ?)")
	if err != nil {
		return
	}
	i.insertObjectStmt = insertObjectStmt
	updateSourceObjectStmt, err := db.Prepare("UPDATE object SET source_score = ?, deleted = ?, compression = ?, encoding = ?, data = ?, kids = ?, num_kids = ?, version = version + 1 WHERE id = ? AND version = ?")
	if err != nil {
		return
	}
	i.updateSourceObject = updateSourceObjectStmt
	insertURL, err := db.Prepare("INSERT INTO urls (url_hash, url, post_id) VALUES (?, ?, ?)")
	if err != nil {
		return
	}
	i.insertURL = insertURL
	insertSourceIDToObjectID, err := db.Prepare("INSERT INTO source_id_to_object_id (source, source_id, object_id) VALUES (?, ?, ?)")
	if err != nil {
		return
	}
	i.insertSourceIDToObjectID = insertSourceIDToObjectID
	getObjectStmt, err := db.Prepare("SELECT id, source, type, score, source_score, deleted, unixtime, compression, encoding, data, kids, num_kids, version FROM object WHERE id = ?")
	if err != nil {
		return
	}
	i.getObject = getObjectStmt
	getObjectIDFromSourceID, err := db.Prepare("SELECT object_id FROM source_id_to_object_id WHERE source = ? AND source_id = ?")
	if err != nil {
		return
	}
	i.getObjectIDFromSourceIDStmt = getObjectIDFromSourceID
	retVal = new(Database)
	*retVal = i
	return
}

func (i *Database) close() {
	// close all statements
	e := reflect.ValueOf(i).Elem()
	t := e.Type()
	for i := 0; i < t.NumField(); i++ {
		f := e.Field(i)
		if f.Type() == reflect.TypeOf((*sql.Stmt)(nil)) {
			value := f.Interface()
			if value != nil {
				value.(*sql.Stmt).Close()
			}
		}
	}
	if i.db != nil {
		i.db.Close()
	}
}

// GetObjectIDFromSourceID get object ID from content source ID
func (i *Database) GetObjectIDFromSourceID(source protocol.SourceID, sourceID []byte) (int64, error) {
	row := i.getObjectIDFromSourceIDStmt.QueryRow(source, sourceID)
	var objectID int64
	err := row.Scan(&objectID)
	return objectID, err
}

// InsertObject insert a dbObject
func (i *Database) InsertObject(obj Object) (err error) {
	objData, err := EncodeData(obj.Data)
	if err != nil {
		return
	}
	objKids, err := EncodeKids(obj.Kids)
	if err != nil {
		return
	}
	_, err = i.insertObjectStmt.Exec(obj.ID, obj.Source, obj.Type, obj.Score, obj.SourceScore, obj.Deleted, obj.UnixTime, obj.Compression, obj.Encoding, objData, objKids, obj.NumKids)
	return
}

// InsertSourceIDToObjectID insert a mapping between id from a content source and object id
func (i *Database) InsertSourceIDToObjectID(objID int64, source protocol.SourceID, sourceID []byte) (err error) {
	_, err = i.insertSourceIDToObjectID.Exec(source, sourceID, objID)
	return err
}

// UpdateSourceObject update object fields that come from content sources
func (i *Database) UpdateSourceObject(obj Object, version int) (err error) {
	objData, err := EncodeData(obj.Data)
	if err != nil {
		return
	}
	objKids, err := EncodeKids(obj.Kids)
	if err != nil {
		return
	}
	_, err = i.updateSourceObject.Exec(obj.SourceScore, obj.Deleted, obj.Compression, obj.Encoding, objData, objKids, obj.NumKids, obj.ID, version)
	return
}

// GetObject get object
func (i *Database) GetObject(objID int64) (obj Object, version int, err error) {
	row := i.getObject.QueryRow(objID)
	var data []byte
	var kids []byte
	err = row.Scan(&obj.ID, &obj.Source, &obj.Type, &obj.Score, &obj.SourceScore, &obj.Deleted, &obj.UnixTime, &obj.Compression, &obj.Encoding, &data, &kids, &obj.NumKids, &version)
	m, err := DecodeData(data, obj.Type)
	if err != nil {
		return
	}
	obj.Data = m
	kidsObj, err := DecodeKids(kids)
	if err != nil {
		return
	}
	obj.Kids = kidsObj
	return
}

// DecodeKids deserialize kids field
func DecodeKids(bytes []byte) (Kids, error) {
	var kids Kids
	buffer := proto.NewBuffer(bytes)
	err := buffer.DecodeMessage(&kids)
	if err != nil {
		return Kids{}, err
	}
	return kids, nil
}

// EncodeKids serialized kids field
func EncodeKids(kids Kids) ([]byte, error) {
	buffer := proto.NewBuffer(nil)
	err := buffer.EncodeMessage(&kids)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// DecodeData decode data part of objects
func DecodeData(data []byte, t protocol.ObjectType) (m proto.Message, err error) {
	buf := proto.NewBuffer(data)
	switch t {
	case protocol.User:
		var u User
		err = buf.DecodeMessage(&u)
		if err != nil {
			return
		}
		m = &u
	case protocol.LinkPost:
		fallthrough
	case protocol.TextPost:
		fallthrough
	case protocol.Comment:
		fallthrough
	case protocol.Job:
		fallthrough
	case protocol.Poll:
		fallthrough
	case protocol.PollOpt:
		var p Post
		err = buf.DecodeMessage(&p)
		if err != nil {
			return
		}
		m = &p
	default:
		err = fmt.Errorf("Unhandled type %d", t)
		return
	}
	return
}

// EncodeData encoding data part of objects
func EncodeData(m proto.Message) ([]byte, error) {
	buf := proto.NewBuffer(nil)
	err := buf.EncodeMessage(m)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type compressionType uint8

const (
	// None no compression
	None = compressionType(1)
)

type encodingType uint8

const (
	// Protobuf protobuf encoding
	Protobuf = encodingType(1)
)

// Object a content object
type Object struct {
	ID          int64
	Source      protocol.SourceID
	Type        protocol.ObjectType
	Score       int64
	SourceScore int64
	Deleted     bool
	UnixTime    int32
	Compression compressionType
	Encoding    encodingType
	Data        proto.Message
	Kids        Kids
	NumKids     int32
}
