package util

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

func InsertData(values string) {

	insertSql := "INSERT INTO " + tableName + " VALUES(" + values + ")"

	fmt.Println(insertSql)
	row, error := db.Query(insertSql)
	if error != nil {
		fmt.Println("INSERT ERROR:", error, ", DATA:", values)
	}
	if row != nil {
		defer row.Close()
	}
}
