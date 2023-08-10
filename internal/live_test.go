package internal

import (
	"context"
	"database/sql"
	"fmt"
	"go-db-compare/configs"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func TestRemoveSchemaIrrelevantElementsOK(t *testing.T) {
	tableSchema := &sql.NullString{
		String: "AUTO_INCREMENT=123 cd",
		Valid:  true,
	}
	err := removeSchemaIrrelevantElements(tableSchema)
	assert.NoError(t, err, "expected no error, got %v", err)
	assert.EqualValues(t, "cd", tableSchema.String)
}

func TestRemoveSchemaIrrelevantElements(t *testing.T) {
	tableSchema := &sql.NullString{
		Valid: false,
	}
	err := removeSchemaIrrelevantElements(tableSchema)
	assert.NoError(t, err, "expected no error, got %v", err)
}

func TestRemoveSchemaIrrelevantElementsanything(t *testing.T) {
	tableSchema := &sql.NullString{
		String: "=.*",
		Valid:  true,
	}
	err := removeSchemaIrrelevantElements(tableSchema)
	assert.NoError(t, err, "expected no error, got %v", err)
	assert.EqualValues(t, "=.*", tableSchema.String)
}

func TestRemoveSchemaIrrelevantElementsemptystring(t *testing.T) {
	tableSchema := &sql.NullString{
		String: "",
		Valid:  true,
	}
	err := removeSchemaIrrelevantElements(tableSchema)
	assert.NoError(t, err, "expected no error, got %v", err)
	assert.EqualValues(t, "", tableSchema.String)
}

func TestRemoveSchemaIrrelevantElements_cd(t *testing.T) {
	tableSchema := &sql.NullString{
		String: "Auto_increment=123 cd",
		Valid:  true,
	}
	err := removeSchemaIrrelevantElements(tableSchema)
	assert.NoError(t, err, "expected no error, got %v", err)
	assert.EqualValues(t, "Auto_increment=123 cd", tableSchema.String)
}

func getMockData(ctx context.Context) (*databaseConn, sqlmock.Sqlmock, error) {
	var db *sql.DB
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, nil, err
	}
	conn := &databaseConn{
		connection: db,
		config:     &mysql.Config{DBName: ""},
	}
	return conn, mock, err
}
func TestMakeQueryGetTableDataOK(t *testing.T) {
	config, err := configs.GetConf("../config.yaml")
	assert.NoError(t, err, "error creating config: %v", err)
	ctx := context.WithValue(context.Background(), contextKeyConfig, config)

	conn, mock, err := getMockData(ctx)
	assert.NoError(t, err, "error creating mock: %v", err)

	mock.ExpectBegin()
	conn.tx, err = conn.connection.BeginTx(ctx, &sql.TxOptions{})
	assert.NoError(t, err, "error creating database transaction: %v", err)

	mock.ExpectQuery(fmt.Sprintf(stmtGetTableColumns, "tableName", conn.config.DBName)).
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE"}).
			AddRow("id", "int").
			AddRow("email", "string").
			AddRow("name", "string"))

	query, err := makeQueryGetTableData(ctx, conn, "tableName")
	assert.NoError(t, err, "error making query for getting data: %v", err)

	expectedQuery := "SELECT `id`, `email`, `name` FROM `tableName`"

	assert.EqualValues(t, expectedQuery, query)
}

func TestGetDataFromTableOK(t *testing.T) {
	config, err := configs.GetConf("../config.yaml")
	assert.NoError(t, err, "error creating config: %v", err)
	ctx := context.WithValue(context.Background(), contextKeyConfig, config)

	conn, mock, err := getMockData(ctx)
	assert.NoError(t, err, "error creating mock: %v", err)

	mock.ExpectBegin()
	conn.tx, err = conn.connection.BeginTx(ctx, &sql.TxOptions{})
	assert.NoError(t, err, "error creating database transaction: %v", err)

	tableName := "tableName"

	mock.ExpectQuery(fmt.Sprintf(stmtGetTableColumns, tableName, conn.config.DBName)).
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE"}).
			AddRow("id", "int").
			AddRow("email", "string").
			AddRow("name", "string"))

	rows := sqlmock.NewRows([]string{"id", "email", "name"}).
		AddRow(1, nil, "Test Name 1").
		AddRow(2, "test2@test.de", "Test Name 2").
		AddRow(3, "", "Test Name 3")

	mock.ExpectPrepare("SELECT `id`, `email`, `name` FROM `tableName`").ExpectQuery().WillReturnRows(rows)

	results, columns, err := getDataFromTable(ctx, conn, tableName)
	assert.NoError(t, err, "error getting data from table: %v", err)

	expectedResults := []string{"1,nil,Test Name 1", "2,test2@test.de,Test Name 2", "3,,Test Name 3"}
	expectedColmns := []string{"id", "email", "name"}

	assert.EqualValues(t, expectedResults, results)
	assert.EqualValues(t, expectedColmns, columns)
}
func TestGetTablesOK(t *testing.T) {
	config, err := configs.GetConf("../config.yaml")
	assert.NoError(t, err, "error creating config: %v", err)
	ctx := context.WithValue(context.Background(), contextKeyConfig, config)

	conn, mock, err := getMockData(ctx)
	assert.NoError(t, err, "error creating mock: %v", err)

	mock.ExpectBegin()
	conn.tx, err = conn.connection.BeginTx(ctx, &sql.TxOptions{})
	assert.NoError(t, err, "error creating database transaction: %v", err)

	mock.ExpectQuery(stmtGetAllTables).
		WillReturnRows(sqlmock.NewRows([]string{"Tables_in_mydb", "Table_type"}).
			AddRow("ACCOUNT", "BASE TABLE").
			AddRow("ADDRESS", "BASE TABLE"))

	err = conn.getTables(ctx)
	assert.NoError(t, err, "error getting tables")

	expectedResults := []fullTable{
		{
			Name: "ACCOUNT",
			Type: "BASE TABLE",
		},
		{
			Name: "ADDRESS",
			Type: "BASE TABLE",
		},
	}
	assert.EqualValues(t, expectedResults, conn.tables)
}
