package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/reid00/kv_engine"
	"github.com/reid00/kv_engine/logger"
	"github.com/tidwall/redcon"
)

var (
	errClientIsNil = errors.New("client conn is nil")
)

var (
	defaultDBPath            = filepath.Join("/tmp", "rosedb")
	defaultHost              = "127.0.0.1"
	defaultPort              = "5200"
	defaultDataBasesNum uint = 16
)

const (
	dbName = "rosedb-%04d"
)

func init() {
	// print basic infomation
	path, _ := filepath.Abs("resource/baner.txt")
	banner, _ := os.ReadFile(path)
	fmt.Println(string(banner))
}

type Server struct {
	dbs    map[int]*kv_engine.RoseDB
	svr    *redcon.Server
	singal chan os.Signal
	opts   ServerOptions
	mu     *sync.RWMutex
}

type ServerOptions struct {
	dbPath    string
	host      string
	port      string
	databases uint
}

func main() {

	serverOpts := new(ServerOptions)

	flag.StringVar(&serverOpts.dbPath, "dbpath", defaultDBPath, "db path")
	flag.StringVar(&serverOpts.host, "host", defaultHost, "server host")
	flag.StringVar(&serverOpts.port, "port", defaultPort, "server port")
	flag.UintVar(&serverOpts.databases, "database", defaultDataBasesNum, "the number of database")
	flag.Parse()

	path := filepath.Join(serverOpts.dbPath, fmt.Sprintf(dbName, 0))
	opts := kv_engine.DefaultOptions(path)

	now := time.Now()
	db, err := kv_engine.Open(opts)
	if err != nil {
		// panic("open rosedb err, failed to start server")
		logger.Errorf("open rosedb err, fail to start server. %v", err)
		return
	}
	logger.Infof("open db from [%s] successfully, time cost: %v", serverOpts.dbPath, time.Since(now))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)

	dbs := make(map[int]*kv_engine.RoseDB)

	dbs[0] = db

	// init and start server
	svr := &Server{
		dbs:    dbs,
		singal: sig,
		opts:   *serverOpts,
		mu:     new(sync.RWMutex),
	}

	addr := svr.opts.host + ":" + svr.opts.port

	redServer := redcon.NewServerNetwork("tcp", addr, execClientCommand, svr.redconAccept,
			func (conn redcon.Conn, err error) {

			},
		)

	svr.svr = redServer
	go svr.listen()
	<- svr.singal
	svr.stop()
}

func (svr *Server) listen()  {
	logger.Infof("rosedb server is running, ready to accept conection")
	if err := svr.svr.ListenAndServe(); err != nil {
		logger.Fatalf("listen and serve err, fail to start. %v\n", err)	
		return 
	}
}

func (svr *Server) stop()  {
	for _, db := range svr.dbs {
		if err := db.Close(); err != nil {
			logger.Errorf("close db err: %v", err)	
		}
	}

	if err := svr.svr.Close(); err != nil {
		logger.Errorf("close server err: %v", err)	
	}
	logger.Info("rosedb is ready to exit, byt byte...")
}

func (svr *Server) redconAccept(conn redcon.Conn) bool {
	cli := new(Client)
	cli.svr = svr
	svr.mu.RLock()
	conn.SetContext(cli)
	return true
}
