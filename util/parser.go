package util

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
	"regexp"
	"strings"
	"time"
)

//columnName: column name
//columnType: attribute type
//length: column fixed length
//int64Sign: if value is unique and signed number
//uint64Sign: if value is unique and unsinged number
type columnInfo struct {
	columnName string
	columnType string
	length     int
	unique     bool
	unsigned   bool
	int64Sign  int64
	uint64Sign uint64
	stringNum  int64
	timeStamp  time.Time
}

type tableInfo struct {
	tableName    string
	columnsArray []columnInfo
}

/*
type timeStampObj struct {
	year int
	month int
	day int
	hour int
	minute int
	second int
}
*/

//parse config file
type Config struct {
	Mysql     mysqlConfig
	Sql       sqlStmt
	Data      dataInfo
	Goroutine goroutineConfig
}

type mysqlConfig struct {
	Ip        string
	Port      string
	Database  string
	Tablename string
	Username  string
	Password  string
}

type sqlStmt struct {
	Tablestmt string
	Indexstmt string
}

type dataInfo struct {
	Datanumber int64
}
type goroutineConfig struct {
	Goroutinenum int
}

var (
	ip           string
	port         string
	dataBase     string
	tableName    string
	userName     string
	passWord     string
	tableStmt    string
	indexStmt    string
	goroutineNum int
	dataNumber   int64

	table tableInfo
)

//parsing SQL statements
func Parser() {

	//parse config.toml
	var conf Config
	_, err := toml.DecodeFile("../config.toml", &conf)
	if err != nil {
		fmt.Println("CONFIG TOML ERROR:", err)
		return
	}

	ip = conf.Mysql.Ip
	port = conf.Mysql.Port
	dataBase = conf.Mysql.Database
	tableName = conf.Mysql.Tablename
	userName = conf.Mysql.Username
	passWord = conf.Mysql.Password
	tableStmt = conf.Sql.Tablestmt
	indexStmt = conf.Sql.Indexstmt
	dataNumber = conf.Data.Datanumber
	goroutineNum = conf.Goroutine.Goroutinenum

	Parser := parser.New()
	st, err := Parser.ParseOneStmt(tableStmt, "", "")
	if err != nil {
		fmt.Println("ERROR:", err)
	}
	cs, _ := st.(*ast.CreateTableStmt)

	//parse table name
	table.tableName = cs.Table.Name.String()
	fmt.Println(table.tableName)
	//parse cloumn name and type
	for i, _ := range cs.Cols {
		var column columnInfo
		column.columnName = cs.Cols[i].Name.Name.String()
		column.length = cs.Cols[i].Tp.Flen
		re := regexp.MustCompile(`((?P<Type>(\w*))\s*\(\d*\)?)`)
		match := re.FindStringSubmatch(cs.Cols[i].Tp.String())
		if match != nil {
			result := make(map[string]string)
			for i, name := range re.SubexpNames() {
				if i != 0 {
					result[name] = match[i]
				}
			}
			column.columnType = strings.ToUpper(result["Type"])
		} else {
			column.columnType = strings.ToUpper(cs.Cols[i].Tp.String())
			fmt.Println(column.columnType)
		}
		if len(cs.Cols[i].Options) != 0 && cs.Cols[i].Options[0].Tp == 1 {
			column.unique = true
		}
		table.columnsArray = append(table.columnsArray, column)
	}

	//parse index
	if indexStmt != "" {
		st, err = Parser.ParseOneStmt(indexStmt, "", "")
		if err != nil {
			fmt.Println("ERROR:", err)
		}
		indexCs, _ := st.(*ast.CreateIndexStmt)

		if indexCs.Unique {
			for i, _ := range table.columnsArray {
				for j, _ := range indexCs.IndexColNames {
					if indexCs.IndexColNames[j].Column.Name.String() == table.columnsArray[i].columnName {
						fmt.Println(indexCs.IndexColNames[j].Column.Name.String())
						table.columnsArray[i].unique = true
					}
				}

			}
		}
	}

	fmt.Println(table)
}
