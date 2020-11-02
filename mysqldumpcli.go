package main

import (
	"gomysqldump/gomysqldump"
)

func main() {
	databaseConfig := gomysqldump.DatabaseConfig{
		User:     "root",
		Password: "root",
		Host:     "localhost",
		Database: "sakila_restored", // ToDo: Have to sepecify it twice
		Port:     3306,
	}
	dumpOptions := gomysqldump.DumpOptions{
		Database:       "sakila_restored",
		FileName:       "sakila_restored.sql",
		Dir:            "/Users/rajanp/work/my-github/_test",
		ConfigFile:     "/Users/rajanp/work/my-github/go-mysqldump/options.json",
		DatabaseConfig: databaseConfig,
		DumpSchema:     true,
		DumpData:       true,
	}

	dumper := gomysqldump.NewDumper(dumpOptions)
	dumper.Dump()
}
