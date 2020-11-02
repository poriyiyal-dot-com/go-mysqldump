package gomysqldump

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"text/template"
	"time"
)

type TemplateVars struct {
	Name      string
	SchemaSQL string
	Values    string
}

type dump struct {
	DumpVersion   string
	ServerVersion string
	CompleteTime  string
	DumpSchema    bool
	DumpData      bool
}

const version = "0.2.2"

const headerTemplate = `-- Go SQL Dump %s
-- Backup Started:	%s
-- ------------------------------------------------------
-- Server version	%s

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
`

const tableSchemaTemplate = `
DROP TABLE IF EXISTS {{ .Name }};
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
{{ .SchemaSQL }};
/*!40101 SET character_set_client = @saved_cs_client */;

`

const valuesTemplate = `--
-- Dumping data for table {{ .Name }}
--

LOCK TABLES {{ .Name }} WRITE;
/*!40000 ALTER TABLE {{ .Name }} DISABLE KEYS */;
{{ if .Values }}
INSERT INTO {{ .Name }} VALUES {{ .Values }};
{{ end }}
/*!40000 ALTER TABLE {{ .Name }} ENABLE KEYS */;
UNLOCK TABLES;
`

// InitTemplates - Initialize the templates
func (d *Dumper) InitTemplates() {
	tst, err := template.New("tableSchemaTemplate").Parse(tableSchemaTemplate)

	if err != nil {
		fmt.Println(err)
		panic("Unable to load table schema template")
	}

	vt, err := template.New("valuesTemplate").Parse(valuesTemplate)

	if err != nil {
		fmt.Println(err)
		panic("Unable to load table valuesTemplate")
	}

	d.schemaTemplate = tst
	d.valuesTemplate = vt
}

// Dump - My Dump Method
func (d *Dumper) Dump() {
	d.writeHeader()

	tables, err := d.getTablesToBeDumped()
	if err != nil {
		fmt.Println("Error occured while getting tables")
	}

	for _, table := range tables {
		d.dumpTable(table)
	}

	d.writeFooter()
}

func (d *Dumper) writeHeader() {
	startTime := time.Now().String()
	serverVersion, err := getServerVersion(d.db)

	if err != nil {
		panic("Unable to get server version")
	}

	fmt.Println("Server Version:", serverVersion)

	data := fmt.Sprintf(headerTemplate, "1.0.0", startTime, serverVersion)

	d.fileWriter.WriteContent(data)
}

func (d *Dumper) writeFooter() {
	completedTime := time.Now().String()

	msg := fmt.Sprintf("\n\n-- Backup Completed: %s", completedTime)
	d.fileWriter.WriteContent(msg)
}

func (d *Dumper) getTablesToBeDumped() ([]string, error) {
	tables := make([]string, 0)

	// Get table list
	rows, err := d.db.Query("select TABLE_NAME from information_schema.tables where TABLE_SCHEMA='sakila' AND TABLE_TYPE <> 'VIEW'")
	if err != nil {
		return tables, err
	}
	defer rows.Close()

	// Read result
	for rows.Next() {
		var table sql.NullString
		if err := rows.Scan(&table); err != nil {
			return tables, err
		}
		tables = append(tables, table.String)
	}

	return tables, rows.Err()
}

func (d *Dumper) dumpTable(tableName string) {
	if d.dumpOptions.DumpSchema == true {
		d.dumpSchema(tableName)
	}

	if d.dumpOptions.DumpData == true {
		d.dumpData(tableName)
	}
}

func (d *Dumper) dumpSchema(tableName string) (string, error) {
	sql, err := createTableSQL(d.db, tableName)

	if err != nil {
		return "", err
	}

	data := TemplateVars{
		Name:      tableName,
		SchemaSQL: sql,
	}

	d.fileWriter.WriteTemplatedContent(d.schemaTemplate, data)

	return "", err
}

func (d *Dumper) dumpData(tableName string) (string, error) {
	sql, err := createTableValues(d.db, tableName)

	data := TemplateVars{
		Name:   tableName,
		Values: sql,
	}

	d.fileWriter.WriteTemplatedContent(d.valuesTemplate, data)

	return "", err
}

func getTables(db *sql.DB) ([]string, error) {
	tables := make([]string, 0)

	// Get table list
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return tables, err
	}
	defer rows.Close()

	// Read result
	for rows.Next() {
		var table sql.NullString
		if err := rows.Scan(&table); err != nil {
			return tables, err
		}
		tables = append(tables, table.String)
	}
	return tables, rows.Err()
}

func getServerVersion(db *sql.DB) (string, error) {
	var serverVersion sql.NullString
	if err := db.QueryRow("SELECT version()").Scan(&serverVersion); err != nil {
		fmt.Println("Error getting server version:", err)
		return "", err
	}
	return serverVersion.String, nil
}

func createTableSQL(db *sql.DB, name string) (string, error) {
	// Get table creation SQL
	var table_return sql.NullString
	var table_sql sql.NullString
	err := db.QueryRow("SHOW CREATE TABLE "+name).Scan(&table_return, &table_sql)

	if err != nil {
		return "", err
	}
	if table_return.String != name {
		return "", errors.New("Returned table is not the same as requested table")
	}

	return table_sql.String, nil
}

func createTableValues(db *sql.DB, name string) (string, error) {
	type colType struct {
		scanType       reflect.Type
		dbType         string
		sanitizeString bool
		sanitizeBlob   bool
	}
	colTypeMap := map[int]colType{}

	fmt.Println("SELECT * FROM " + name)

	// Get Data
	rows, err := db.Query("SELECT * FROM " + name)
	if err != nil {
		fmt.Println("ERORR")
		fmt.Println(err)
		return "", err
	}
	defer rows.Close()

	// Get columns
	columns, err := rows.Columns()
	if err != nil {
		fmt.Println("ERORR")
		return "", err
	}
	if len(columns) == 0 {
		fmt.Println("ERORR")
		return "", errors.New("No columns in table " + name + ".")
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		fmt.Println("ERORR")
		return "", err
	}

	fmt.Println("Col Types:", colTypes)

	for i, ct := range colTypes {
		sanitizeString := false
		sanitizeBlob := false

		fmt.Println(ct.DatabaseTypeName())

		if ct.DatabaseTypeName() == "BLOB" || ct.DatabaseTypeName() == "GEOMETRY" {
			sanitizeBlob = true
		}

		if ct.DatabaseTypeName() == "VARCHAR" {
			sanitizeString = true
		}

		colTypeMap[i] = colType{
			scanType:       ct.ScanType(),
			dbType:         ct.DatabaseTypeName(),
			sanitizeBlob:   sanitizeBlob,
			sanitizeString: sanitizeString,
		}
	}

	fmt.Println(colTypeMap)

	// Read data
	data_text := make([]string, 0)
	for rows.Next() {
		// Init temp data storage

		//ptrs := make([]interface{}, len(columns))
		//var ptrs []interface {} = make([]*sql.NullString, len(columns))

		data := make([]*sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i, _ := range data {
			ptrs[i] = &data[i]
		}

		// Read data
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}

		dataStrings := make([]string, len(columns))
		index := -1

		for key, value := range data {
			index++
			sanitizeString := colTypeMap[index].sanitizeString
			sanitizeBlob := colTypeMap[index].sanitizeBlob

			if value != nil && value.Valid {
				if sanitizeString == true {
					dataStrings[key] = "'" + value.String + "'"
				} else if sanitizeBlob == true {
					// fmt.Println(value.String)
					dataStrings[key] = "null"

					src := []byte(value.String)
					dst := make([]byte, hex.EncodedLen(len(src)))
					hex.Encode(dst, src)

					fmt.Printf("%s\n", dst)

					dataStrings[key] = "x'" + string(dst) + "'"
				} else {
					dataStrings[key] = "'" + value.String + "'"
				}

			} else {
				dataStrings[key] = "null"
			}
		}

		data_text = append(data_text, "("+strings.Join(dataStrings, ",")+")")
	}

	return strings.Join(data_text, ","), rows.Err()
}
