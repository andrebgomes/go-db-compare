package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

const (
	stmtGetAllTables        = "SHOW FULL TABLES"
	stmtGetTableInformation = "SHOW CREATE TABLE `%s` "
	stmtGetTableData        = "SELECT %s FROM `%s`"
	stmtGetTableColumns     = `SELECT COLUMN_NAME, DATA_TYPE 
	FROM INFORMATION_SCHEMA.COLUMNS 
	WHERE TABLE_NAME = '%s' AND TABLE_SCHEMA = '%s';`

	tableTypeBaseTable = "BASE TABLE"
	tableTypeView      = "VIEW"
)

var (
	errNoColumns error = fmt.Errorf("no columns found")
)

// fullTable holds the name and type of a databse table. its meant to be used when querying with stmtGetAllTables
type fullTable struct {
	Name string
	Type string // "BASE TABLE", "VIEW"
}

func runStrategyLive(ctx context.Context) error {
	config := getConfigFromContext(ctx)
	// Connect to databases
	database1, err := openDatabaseConnection(ctx, config.Database1)
	if err != nil {
		return err
	}
	database2, err := openDatabaseConnection(ctx, config.Database2)
	if err != nil {
		return err
	}

	// Compare databases
	err = compareDatabases(ctx, database1, database2)
	if err != nil {
		return err
	}

	return nil
}

func compareDatabases(ctx context.Context, db1 *databaseConn, db2 *databaseConn) error {
	var err error

	// Begin transaction
	db1.tx, err = db1.connection.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	db2.tx, err = db2.connection.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	// Get tables from both databases
	if err := db1.getTables(ctx); err != nil {
		return err
	}
	if err := db2.getTables(ctx); err != nil {
		return err
	}

	// Compare number of tables
	if len(db1.tables) != len(db2.tables) {
		return fmt.Errorf("number of tables doesn't match. %s -> %d, %s -> %d",
			db1.config.DBName, len(db1.tables), db2.config.DBName, len(db2.tables))
	}

	// Compare schemas
	if err := compareSchema(ctx, db1, db2); err != nil {
		return fmt.Errorf("schema error: %v", err)
	}

	// Compare data
	if err := compareData(ctx, db1, db2); err != nil {
		return fmt.Errorf("data error: %v", err)
	}

	return nil
}

func compareSchema(ctx context.Context, db1 *databaseConn, db2 *databaseConn) error {
	// Go through every table and check their schema
	for i := 0; i < len(db1.tables); i++ {
		// Compare tables names
		if db1.tables[i] != db2.tables[i] {
			return fmt.Errorf("table names don't match")
		}
		table := db1.tables[i]

		// Get table info
		var tableSQL1, tableSQL2, doesntMatter sql.NullString
		query := fmt.Sprintf(stmtGetTableInformation, table.Name)
		queryStmtDB1, err := db1.tx.PrepareContext(ctx, query)
		if err != nil {
			return err
		}
		queryStmtDB2, err := db2.tx.PrepareContext(ctx, query)
		if err != nil {
			return err
		}

		// We expect to have two fields from the select if the table type is tableTypeBaseTable.
		// If this table type is actually tableTypeView, we expect to have four fields from the select.
		//
		// (note that we can not do the queries before the if statement because if two queries occur
		// without a scan in between, a busy error occurs)
		if table.Type == tableTypeView {
			rowDB1 := queryStmtDB1.QueryRowContext(ctx)
			if err := rowDB1.Scan(&doesntMatter, &tableSQL1, &doesntMatter, &doesntMatter); err != nil {
				return err
			}
			rowDB2 := queryStmtDB2.QueryRowContext(ctx)
			if err := rowDB2.Scan(&doesntMatter, &tableSQL2, &doesntMatter, &doesntMatter); err != nil {
				return err
			}
		} else {
			rowDB1 := queryStmtDB1.QueryRowContext(ctx)
			if err := rowDB1.Scan(&doesntMatter, &tableSQL1); err != nil {
				return err
			}
			rowDB2 := queryStmtDB2.QueryRowContext(ctx)
			if err := rowDB2.Scan(&doesntMatter, &tableSQL2); err != nil {
				return err
			}
		}

		// Remove irrelevant elements to the schema comparison
		if err := removeSchemaIrrelevantElements(&tableSQL1); err != nil {
			return err
		}
		if err := removeSchemaIrrelevantElements(&tableSQL2); err != nil {
			return err
		}

		// Compare table schema
		if tableSQL1.String != tableSQL2.String {
			return fmt.Errorf("table %s schemas don't match", table.Name)
		}
	}

	return nil
}

func compareData(ctx context.Context, db1 *databaseConn, db2 *databaseConn) error {
	config := getConfigFromContext(ctx)
	// Go through every table and check their data
	for _, table := range db1.tables {
		// Get data from this table for database1
		results1, columnsName, err := getDataFromTable(ctx, db1, table.Name)
		if err != nil {
			if errors.Is(err, errNoColumns) {
				err = nil
				continue
			}
			return err
		}

		// Get data from this table for database2
		results2, _, err := getDataFromTable(ctx, db2, table.Name)
		if err != nil {
			return err
		}

		// Check if number of rows are the same
		if len(results1) != len(results2) {
			return fmt.Errorf("number of rows in table %s doesn't match. %s -> %d, %s -> %d",
				table.Name, db1.config.DBName, len(results1), db2.config.DBName, len(results2))
		}

		// Check if columns values are the same
		for i := 0; i < len(results1); i++ {
			// Check if columns are the same
			if results1[i] == results2[i] {
				continue
			}

			// Columns are not the same, check and print where the difference is
			columns1 := strings.Split(results1[i], ",")
			columns2 := strings.Split(results2[i], ",")

			for j := 0; j < len(columns1); j++ {
				if columns1[j] != columns2[j] {
					s := `the value from table %s in column %s on row %d is not the same
					` + config.Database1.Label + `:	'` + columns1[j] + `'
					` + config.Database2.Label + `:	'` + columns2[j] + `'`
					return fmt.Errorf(s, table.Name, columnsName[j], i+1)
				}
			}
		}
	}

	return nil
}

// getDataFromTable returns the existing data from given table and its columns.
//
// The data will be an array of strings, each string represents a row with every column seperated by ",".
// If a value is null, then the string will contain "nil" instead.
// example:
// [1,"s",,nil], [id,string,emprtyString,nilValue]
func getDataFromTable(ctx context.Context, db *databaseConn, table string) ([]string, []string, error) {
	// Get query with right columns to fetch table data
	query, err := makeQueryGetTableData(ctx, db, table)
	if err != nil {
		return nil, nil, err
	}

	// Perform query
	queryStmt, err := db.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	rows, err := queryStmt.QueryContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	// Get columns returned
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	// Scan query results
	results := []string{}
	for rows.Next() {
		strs := make([]*string, len(columns))
		vals := make([]interface{}, len(columns))
		for i := range vals {
			vals[i] = &strs[i]
		}
		if err := rows.Scan(vals...); err != nil {
			return nil, nil, err
		}
		results = append(results, strings.Join(removePointersFromStrings(strs), ","))
	}

	sort.Strings(results)

	return results, columns, nil
}

func removePointersFromStrings(pointers []*string) []string {
	res := []string{}
	for _, p := range pointers {
		if p == nil {
			res = append(res, "nil")
		} else {
			res = append(res, *p)
		}
	}
	return res
}

// getTables inserts into the struct the existing tables in given database that are not to be ignored.
func (db *databaseConn) getTables(ctx context.Context) error {
	db.tables = make([]fullTable, 0)

	rows, err := db.tx.QueryContext(ctx, stmtGetAllTables)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var tName, tType sql.NullString
		if err := rows.Scan(&tName, &tType); err != nil {
			return err
		}

		if tName.Valid && tType.Valid && !getConfigFromContext(ctx).IsTableToBeIgnored(tName.String) {
			db.tables = append(db.tables, fullTable{
				Name: tName.String,
				Type: tType.String,
			})
		}
	}
	return rows.Err()
}

// removeSchemaIrrelevantElements removes the irrelevant elements from the given string.
// elements being removed:
// - auto increment
func removeSchemaIrrelevantElements(tableSchema *sql.NullString) error {
	// If not valid, do nothing
	if !tableSchema.Valid {
		return nil
	}

	// Remove auto increment
	re := regexp.MustCompile("AUTO_INCREMENT=.* ")
	tableSchema.String = re.ReplaceAllString(tableSchema.String, "")

	return nil
}

// makeQueryGetTableData returns the query to fetch the data from given table.
// The query will contain only the columns that are not to be ignored.
func makeQueryGetTableData(ctx context.Context, db *databaseConn, table string) (string, error) {
	// Get columns of table
	rows, err := db.tx.QueryContext(ctx, fmt.Sprintf(stmtGetTableColumns, table, db.config.DBName))
	if err != nil {
		return "", err
	}
	defer rows.Close()

	// Scan query results
	columns := []string{}
	for rows.Next() {
		var column, dataType string
		if err := rows.Scan(&column, &dataType); err != nil {
			return "", err
		}

		// Append columns if they are not to be ignored
		conf := getConfigFromContext(ctx)
		if !conf.IsColumnToBeIgnored(table, column) && !conf.IsTypeToBeIgnored(dataType) {
			columns = append(columns, column)
		}
	}

	if len(columns) == 0 {
		return "", errNoColumns
	}

	// Build the string with the columns
	var columnsBuilder strings.Builder
	columnsBuilder.WriteString(fmt.Sprintf("`%s`", columns[0]))

	if len(columns) > 1 {
		// Make columns string to use in the query
		for i := 1; i < len(columns); i++ {
			columnsBuilder.WriteString(fmt.Sprintf(", `%s`", columns[i]))
		}
	}

	// Make final query
	query := fmt.Sprintf(stmtGetTableData, columnsBuilder.String(), table)

	return query, nil
}
