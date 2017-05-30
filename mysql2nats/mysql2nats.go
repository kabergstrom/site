package main

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/kabergstrom/site/protocol"
	"github.com/kabergstrom/site/protocol/subjects"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"github.com/siddontang/go-mysql/canal"
	_ "github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
)

type myEventHandler struct {
	canal.DummyEventHandler
	nats stan.Conn
	pos  mysql.Position
}

func (h *myEventHandler) OnXID(pos mysql.Position) error {
	h.pos = pos
	return nil
}
func (h *myEventHandler) OnRow(e *canal.RowsEvent) error {
	if e.Table.Name == "object" {
		pkColumn := e.Table.GetPKColumn(0)
		colIndex := e.Table.FindColumn(pkColumn.Name)
		for _, row := range e.Rows {
			pk := row[colIndex]
			publishPos := h.pos
			mod := protocol.ObjectModified{
				Id:        pk.(int64),
				MysqlFile: publishPos.Name,
				MysqlPos:  publishPos.Pos,
			}

			modBuf, err := proto.Marshal(&mod)
			if err != nil {
				log.Fatal(err)
			}
			publishStart := time.Now()
			if err = h.nats.Publish(subjects.ObjectsModified, modBuf); err != nil {
				errors.Wrapf(err, "Error publishing object modification for pk %d at pos %s %d", pk.(int64), publishPos.Name, publishPos.Name)
				return err
			}
			log.Infof("%s pk %v publish %s\n", e.Action, pk, time.Now().Sub(publishStart))
		}
	}
	return nil
}

func (h *myEventHandler) String() string {
	return "MyEventHandler"
}
func main() {
	nats, err := stan.Connect("test-cluster", "mysql2nats")
	if err != nil {
		log.Fatal(err)
	}
	defer nats.Close()
	// get starting position
	startPos := mysql.Position{}
	{
		positionChan := make(chan mysql.Position)
		sub, err := nats.Subscribe(subjects.ObjectsModified, func(m *stan.Msg) {
			var objMod protocol.ObjectModified
			if err := proto.Unmarshal(m.Data, &objMod); err != nil {
				log.Fatal(err)
			}
			if objMod.MysqlFile != "" {
				positionChan <- mysql.Position{
					Name: objMod.MysqlFile,
					Pos:  objMod.MysqlPos,
				}
			}
		}, stan.StartAt(pb.StartPosition_First))
		if err != nil {
			log.Fatal(err)
		}
		oldPos := startPos
		timeoutChecker := time.NewTicker(time.Second)
	Loop:
		for {
			select {
			case pos := <-positionChan:
				startPos = pos
			case <-timeoutChecker.C:
				if startPos.Name == oldPos.Name && startPos.Pos == oldPos.Pos {
					timeoutChecker.Stop()
					break Loop
				}
				oldPos = startPos
			}
		}
		sub.Unsubscribe()
	}
	cfg := canal.NewDefaultConfig()
	cfg.Addr = "127.0.0.1:3306"
	cfg.User = "root"
	cfg.Password = "test"
	cfg.Flavor = "mysql"
	// We only care table canal_test in test db
	cfg.Dump.TableDB = "site"
	cfg.Dump.Tables = []string{"object"}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Register a handler to handle RowsEvent
	c.SetEventHandler(&myEventHandler{nats: nats})

	if startPos.Name == "" {
		fmt.Printf("Could not read start position from nats streaming server, starting from current..")
		err = c.Start()
	} else {
		fmt.Printf("Resuming from pos %s %d", startPos.Name, startPos.Pos)
		err = c.StartFrom(startPos)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	select {
	case <-c.Ctx().Done():
	}
	if c.Ctx().Err() != nil {
		errors.Wrap(c.Ctx().Err(), "Canal done with error")
	}
}
