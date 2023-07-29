package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"
	"ycsb/common"
	"ycsb/go-ycsb/pkg/client"
	"ycsb/go-ycsb/pkg/measurement"
	"ycsb/go-ycsb/pkg/prop"
	"ycsb/go-ycsb/pkg/util"
	_ "ycsb/go-ycsb/pkg/workload"
	"ycsb/go-ycsb/pkg/ycsb"

	_ "ycsb/go-ycsb/db/redis"

	"github.com/magiconair/properties"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type CmdServer struct {
	id         int32
	startId    int64
	interval   int32
	ycsbClient *client.Client
	common.UnimplementedYCSBServerServer
}

var (
	propertyFiles  []string
	propertyValues []string
	dbName         string
	tableName      string

	globalContext context.Context
	globalCancel  context.CancelFunc

	globalDB       ycsb.DB
	globalWorkload ycsb.Workload
	globalProps    *properties.Properties
)

func initialGlobal(dbName string, onProperties func()) {
	globalContext, globalCancel = context.WithCancel(context.Background())
	globalProps = properties.NewProperties()
	propertyFiles = append(propertyFiles, "./go-ycsb/workloads/workloadc") ///home/ubuntu/fastba-go/workloadc
	if len(propertyFiles) > 0 {
		globalProps = properties.MustLoadFiles(propertyFiles, properties.UTF8, false)
	}
	measurement.InitMeasure(globalProps)

	if onProperties != nil {
		onProperties()
	}

	if len(tableName) == 0 {
		tableName = globalProps.GetString(prop.TableName, prop.TableNameDefault)
	}

	workloadName := globalProps.GetString(prop.Workload, "core")
	workloadCreator := ycsb.GetWorkloadCreator(workloadName)

	var err error
	if globalWorkload, err = workloadCreator.Create(globalProps); err != nil {
		util.Fatalf("create workload %s failed %v", workloadName, err)
	}
	dbCreator := ycsb.GetDBCreator(dbName)
	if dbCreator == nil {
		util.Fatalf("%s is not registered", dbName)
	}
	if globalDB, err = dbCreator.Create(globalProps); err != nil {
		util.Fatalf("create db %s failed %v", dbName, err)
	}
	globalDB = client.DbWrapper{globalDB}
}

func (cs *CmdServer) GetCmds(ctx context.Context, in *common.CmdsReqeust) (*common.CmdsReply, error) {
	log.Println("GetCmds() is called")
	// genearte payload
	txs := new(common.Transactions)
	for i := 0; i < int(cs.interval); i++ {
		op, table, keyName, fields, values := cs.ycsbClient.Generate(globalContext)
		tx := &common.Transaction{
			Op:      op,
			Table:   table,
			KeyName: keyName,
			Fields:  fields,
			Values:  values,
		}
		txs.Txs = append(txs.Txs, tx)
	}
	txs.ClientId = cs.id
	txs.StartId = cs.startId
	cs.startId += int64(cs.interval)
	payload, _ := proto.Marshal(txs)
	log.Printf("send transactions: %d %d %d\n", txs.ClientId, cs.interval, time.Now().UnixNano()/1000000)
	return &common.CmdsReply{
		Payload: payload,
	}, nil
}

func (cs *CmdServer) ExecuteCmds(ctx context.Context, in *common.ExecuteRequest) (*common.ExecuteReply, error) {
	log.Println("ExecuteCmds() is called")
	paylaod := in.Payload[10:]
	var txs common.Transactions
	proto.Unmarshal(paylaod, &txs)
	for _, tx := range txs.Txs {
		cs.ycsbClient.Execute(globalContext, tx.Op, tx.Table, tx.KeyName, tx.Fields, tx.Values)
	}
	log.Printf("execute transactions: %d %d %d\n", txs.ClientId, cs.interval, time.Now().UnixNano()/1000000)
	// execute
	return &common.ExecuteReply{}, nil
}

func main() {
	id := flag.Int("id", 0, "ycb server id")
	interval := flag.Int("reqnum", 10, "cmds number")
	// flag Parse
	flag.Parse()
	cs := &CmdServer{
		id:       int32(*id),
		interval: int32(*interval),
		startId:  1,
	}
	initialGlobal("redis", func() {
		doTransFlag := "true"
		if !true {
			doTransFlag = "false"
		}
		globalProps.Set(prop.DoTransactions, doTransFlag)
		globalProps.Set(prop.Command, "load")
	})

	cs.ycsbClient = client.NewClient(globalProps, globalWorkload, globalDB)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 8000))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	common.RegisterYCSBServerServer(s, cs)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
