package zysql

import (
	"database/sql"
	"context"
	"sync"
	"github.com/zyfcn/zyLog"
)

var (
	dbs  = make(map[string]map[string]*sql.DB)
	lock sync.Mutex
)

func GetConn(driver string, dataSource string) (*sql.Conn, error) {
	if db, err := GetDb(driver, dataSource); err != nil {
		return nil, err
	} else if conn, err2 := db.Conn(context.Background()); err2 != nil {
		return nil, err2
	} else {
		return conn, nil
	}
}

func GetDb(driver string, dataSource string) (*sql.DB, error) {
	lock.Lock()
	defer lock.Unlock()
	if d, _ := dbs[driver]; d == nil {
		zylog.Print("init map[sql.DB]" + driver)
		dbs[driver] = make(map[string]*sql.DB)
	}
	if db, _ := dbs[driver][dataSource]; db == nil {
		return newDb(driver, dataSource)
	} else if err := db.Ping(); err == nil {
		return db, nil
	} else {
		return newDb(driver, dataSource)
	}
}

func newDb(driver string, dataSource string) (*sql.DB, error) {
	if connect, err := sql.Open(driver, dataSource); err != nil {
		return nil, err
	} else if err = connect.Ping(); err != nil {
		return nil, err
	} else {
		zylog.Print("init sql.DB %s %s\n", driver, dataSource)
		dbs[driver][dataSource] = connect
		return connect, nil
	}
}
