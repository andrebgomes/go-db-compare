package internal

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

func runStrategyDump1(ctx context.Context) error {
	config := getConfigFromContext(ctx)
	// Connect to database
	db, err := openDatabaseConnection(ctx, config.Database1)
	if err != nil {
		return err
	}

	if err := createNcsvs(ctx, db, config.Dir); err != nil {
		return err
	}

	return nil
}

func runStrategyDump2(ctx context.Context) error {
	config := getConfigFromContext(ctx)
	// Connect to database1
	db1, err := openDatabaseConnection(ctx, config.Database1)
	if err != nil {
		return err
	}

	if err := createNcsvs(ctx, db1, config.Dir); err != nil {
		return err
	}

	// Connect to database2
	db2, err := openDatabaseConnection(ctx, config.Database2)
	if err != nil {
		return err
	}

	if err := createNcsvs(ctx, db2, config.Dir2); err != nil {
		return err
	}

	return nil
}

func createNcsvs(ctx context.Context, db *databaseConn, dir string) error {
	var err error

	// Check if dir is writable/exists
	if err := isDirValid(dir); err != nil {
		return err
	}
	if err := hasWritePermissions(dir); err != nil {
		return err
	}

	// Begin transaction
	db.tx, err = db.connection.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	// Get tables
	if err := db.getTables(ctx); err != nil {
		return err
	}

	for _, table := range db.tables {
		if err := createTableNcsv(ctx, db, table.Name, dir); err != nil {
			return err
		}
	}
	return nil
}

func createTableNcsv(ctx context.Context, db *databaseConn, tableName, dir string) error {
	var w *bufio.Writer

	file, err := os.Create(fmt.Sprintf("%s/%s.Ncsv", dir, tableName))
	if err != nil {
		return err
	}
	defer file.Close()

	w = bufio.NewWriter(file)

	// Write Ncsv content
	data, columns, err := getDataFromTable(ctx, db, tableName)
	if err != nil {
		if errors.Is(err, errNoColumns) {
			err = nil
			return nil
		}
		return err
	}

	// Handle columns
	if len(columns) == 0 {
		return nil
	}

	w.WriteString(strings.Join(columns, ","))
	w.WriteString("\n")

	// Handle data
	if len(data) == 0 {
		return nil
	}

	var dumpRow []string
	for _, row := range data {
		dumpRow = append(dumpRow, row)

		w.WriteString(strings.Join(dumpRow, ","))
		w.WriteString("\n")

		dumpRow = nil
	}

	w.Flush()

	return nil
}

// isDirValid returns whether the given file or directory exists and has write permissions
func isDirValid(path string) error {
	file, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("error checking dir: %v", err)
	}

	if !file.IsDir() {
		return fmt.Errorf("destination is not a directory")
	}

	return nil
}

func hasWritePermissions(path string) error {
	if err := unix.Access(path, unix.W_OK); err != nil {
		return fmt.Errorf("dir has no write permissions")
	}
	return nil
}
