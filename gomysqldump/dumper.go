package gomysqldump

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"os"

	// MySQL Driver
	_ "github.com/go-sql-driver/mysql"
)

// Dumper represents a database.
type Dumper struct {
	db             *sql.DB
	fileWriter     *FileWriter
	dumpOptions    DumpOptions
	schemaTemplate *template.Template
	valuesTemplate *template.Template
}

// TableOptions - Table Options
type TableOptions struct {
	WhereClause         string
	IncludedTablesRegex []string
	ExcludedTablesRegex []string
	IncludedTables      []string
	ExcludedTables      []string
}

// DumpOptions - Options to the dumper
type DumpOptions struct {
	Format         string
	Dir            string
	Database       string
	FileName       string
	DumpSchema     bool
	DumpData       bool
	ConfigFile     string
	TableOptions   TableOptions
	DatabaseConfig DatabaseConfig
}

// DatabaseConfig - Database Settings
type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// InitTablesList - Prepare the inclusion exclusion list
func (do *DumpOptions) InitTablesList() {
	fmt.Println("Loading tables list")

	if do.Database == "" {
		panic("Please specify the database name")
	}

	options := map[string]TableOptions{}

	jsonFile, _ := os.Open(do.ConfigFile)
	data, _ := ioutil.ReadAll(jsonFile)

	if err := json.Unmarshal(data, &options); err != nil {
		panic(err)
	}

	do.TableOptions = options[do.Database]

	fmt.Println("Table Options:")
	fmt.Println(do.TableOptions)
}

// NewDumper - Initialize and return the Dumper
func NewDumper(dumpOptions DumpOptions) *Dumper {
	dumper := new(Dumper)

	dumpOptions.InitTablesList()

	fw := NewFileWiter(dumpOptions.Dir, dumpOptions.FileName)
	dumper.dumpOptions = dumpOptions
	dumper.fileWriter = fw

	dumper.InitTemplates()
	dumper.db = getMySQLConnection(dumpOptions.DatabaseConfig)

	return dumper
}

func getMySQLConnection(dbConfig DatabaseConfig) *sql.DB {
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		dbConfig.User,
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.Database)

	db, err := sql.Open("mysql", connectionString)

	if err != nil {
		fmt.Println(err)
		panic("Error opening database:")
	}

	return db
}

// Close - Finalize the dumper object
func (dumper *Dumper) Close() error {
	defer func() {
		dumper.db = nil
	}()
	return dumper.db.Close()
}
