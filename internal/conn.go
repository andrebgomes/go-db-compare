package internal

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go-db-compare/configs"

	"github.com/go-sql-driver/mysql"
)

const (
	dbConnMaxLifetime  = 100
	dbConnMaxIdleConns = 10
)

type databaseConn struct {
	connection *sql.DB
	tx         *sql.Tx

	config *mysql.Config
	tables []fullTable
}

func openDatabaseConnection(ctx context.Context, dbConfig *configs.Database) (*databaseConn, error) {
	// Initialize config
	config := mysql.NewConfig()
	config.User = dbConfig.Username
	config.Passwd = dbConfig.Password
	config.DBName = dbConfig.Database
	config.Net = "tcp"
	config.Addr = fmt.Sprintf("%s:%s", dbConfig.Host, dbConfig.Port)

	// Open connection
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("opening database: %v", err)
	}

	// Set db connections settings
	db.SetConnMaxLifetime(dbConnMaxLifetime)
	db.SetMaxIdleConns(dbConnMaxIdleConns)

	// Verify the connection
	ctxWithTimeout, _ := context.WithTimeout(ctx, time.Second*5)
	if err := db.PingContext(ctxWithTimeout); err != nil {
		return nil, fmt.Errorf("pinging database: %v", err)
	}

	// Initialize connection struct
	d := &databaseConn{
		connection: db,
		config:     config,
	}

	return d, nil
}
