package util

import (
	"database/sql"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	db                                              *sql.DB
	e                                               error
	index                                           int = 0
	outputMutex, tinyMutex, smallMutex, stringMutex sync.Mutex
)

//random string,fixed length
var letterBytes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandString(index int) string {
	length := table.columnsArray[index].length
	b := make([]rune, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

//unique string
func UniqueString(index int) string {
	var str string
	var number uint64 = 1
	stringMutex.Lock()
	length := number << uint64(len(letterBytes))
	for {
		if table.columnsArray[index].stringNum < int64(length) {
			table.columnsArray[index].stringNum++
			for bytesIndex := 0; uint64(bytesIndex) < uint64(len(letterBytes)); bytesIndex++ {
				var mask uint64 = 1
				temp := table.columnsArray[index].stringNum
				if uint64(temp)&(mask<<uint64(bytesIndex)) != 0 {
					str = str + string(letterBytes[bytesIndex])
				}
			}
		}
		if len(str) <= table.columnsArray[index].length {
			break
		} else {
			continue
		}
	}
	stringMutex.Unlock()
	runtime.Gosched()
	return str
}

//char()
func char(index int) string {
	if table.columnsArray[index].unique {
		return UniqueString(index)
	} else {
		return RandString(index)
	}
}

//varchar()
func varChar(index int) string {
	if table.columnsArray[index].unique {
		return UniqueString(index)
	} else {
		return RandString(index)
	}
}

//random number in range [min,max)
func RandNumberInRange(min, max int64) int64 {
	rand.Seed(time.Now().UnixNano())
	return min + rand.Int63n(max-min)
}

//tinyint
func tinyInt(index int) (int64, uint64) {
	if table.columnsArray[index].unique {
		if table.columnsArray[index].unsigned {
			atomic.AddUint64(&table.columnsArray[index].uint64Sign, 1)
			return 0, table.columnsArray[index].uint64Sign
		} else {
			atomic.AddInt64(&table.columnsArray[index].int64Sign, 1)
			return table.columnsArray[index].int64Sign, 0
		}
	} else {
		if table.columnsArray[index].unsigned {
			return 0, uint64(RandNumberInRange(0, 255))
		} else {
			return RandNumberInRange(-127, 128), 0
		}
	}
}

//smallint
func smallInt(index int) (int64, uint64) {
	if table.columnsArray[index].unique {
		if table.columnsArray[index].unsigned {
			atomic.AddUint64(&table.columnsArray[index].uint64Sign, 1)
			return 0, table.columnsArray[index].uint64Sign
		} else {
			atomic.AddInt64(&table.columnsArray[index].int64Sign, 1)
			return table.columnsArray[index].int64Sign, 0
		}
	} else {
		if table.columnsArray[index].unsigned {
			return 0, uint64(RandNumberInRange(0, 65535))
		} else {
			return RandNumberInRange(-32768, 32767), 0
		}
	}
}

//int
func Int(index int) (int64, uint64) {
	if table.columnsArray[index].unique {
		if table.columnsArray[index].unsigned {
			atomic.AddUint64(&table.columnsArray[index].uint64Sign, 1)
			return 0, table.columnsArray[index].uint64Sign
		} else {
			atomic.AddInt64(&table.columnsArray[index].int64Sign, 1)
			return table.columnsArray[index].int64Sign, 0
		}
	} else {
		if table.columnsArray[index].unsigned {
			return 0, uint64(RandNumberInRange(0, 4294967295))
		} else {
			return RandNumberInRange(-2147483648, 2147483647), 0
		}
	}
}

//bigint
func bigInt(index int) (int64, uint64) {
	if table.columnsArray[index].unique {
		if table.columnsArray[index].unsigned {
			atomic.AddUint64(&table.columnsArray[index].uint64Sign, 1)
			return 0, table.columnsArray[index].uint64Sign
		} else {
			atomic.AddInt64(&table.columnsArray[index].int64Sign, 1)
			return table.columnsArray[index].int64Sign, 0
		}
	} else {
		return RandNumberInRange(0, 9223372036854775807), 0

		//if attributeType.unsigned{
		//暂时不支持bigint无符号类型
		//return RandNumberInRange(-9223372036854775808,9223372036854775807)
		//}else{
		//return RandNumberInRange(-9223372036854775808,9223372036854775807),0
		//}
	}
}

//year YYYY
func year(index int) string {
	if table.columnsArray[index].unique {
		yearHours := 8760
		table.columnsArray[index].timeStamp = table.columnsArray[index].timeStamp.Add(time.Hour * time.Duration(yearHours))
		year, _, _ := table.columnsArray[index].timeStamp.UTC().Date()
		return fmt.Sprintf("%d", year)
	} else {
		now := time.Now()
		year, _, _ := now.UTC().Date()
		return fmt.Sprintf("%d", year)
	}
}

//date YYYY-MM-DD
func date(index int) string {
	if table.columnsArray[index].unique {
		yearHours := 24
		table.columnsArray[index].timeStamp = table.columnsArray[index].timeStamp.Add(time.Hour * time.Duration(yearHours))
		year, mon, day := table.columnsArray[index].timeStamp.UTC().Date()
		return fmt.Sprintf("%d-%d-%d", year, mon, day)
	} else {
		now := time.Now()
		year, mon, day := now.UTC().Date()
		return fmt.Sprintf("%d-%d-%d", year, mon, day)
	}

}

//datetime YYYY-MM-DD HH:MM:SS  '1000-01-01 00:00:00' - '9999-12-31 23:59:59'
func dateTime(index int) string {
	return timeStamp(index)
}

//timestamp YYYY-MM-DD HH:MM:SS '1970-01-01 00:00:01' UTC -'2038-01-09 03:14:07' UTC
func timeStamp(index int) string {
	if table.columnsArray[index].unique {
		second := 1
		table.columnsArray[index].timeStamp = table.columnsArray[index].timeStamp.Add(time.Second * time.Duration(second))
		year, mon, day := table.columnsArray[index].timeStamp.UTC().Date()
		hour, min, sec := table.columnsArray[index].timeStamp.UTC().Clock()
		return fmt.Sprintf("%d-%d-%d %02d:%02d:%02d", year, mon, day, hour, min, sec)
	} else {
		now := time.Now()
		year, mon, day := now.UTC().Date()
		hour, min, sec := now.UTC().Clock()
		return fmt.Sprintf("%d-%d-%d %02d:%02d:%02d", year, mon, day, hour, min, sec)
	}
}

func GenerateData(ch chan int) {

	connectConfig := userName + ":" + passWord + "@tcp(" + ip + ":" + port + ")/" + dataBase + "?charset=utf8"
	fmt.Println(connectConfig)

	db, e = sql.Open("mysql", connectConfig)
	if e != nil {
		fmt.Println("MYSQL CONNECT ERROR")
		return
	} else {
		fmt.Println("MYSQL CONNECT SUCCESS")
	}
	for dataCount < dataNumber {
		fmt.Println(dataCount, dataNumber)
		var values string
		for index, columnIn := range table.columnsArray {
			if columnIn.columnType == "TINYINT" {
				if columnIn.unsigned {
					_, v := tinyInt(index)
					values = values + strconv.FormatUint(v, 10) + ","
				} else {
					v, _ := tinyInt(index)
					values = values + strconv.FormatInt(v, 10) + ","
				}
			}
			if columnIn.columnType == "SMALLINT" {
				if columnIn.unsigned {
					_, v := smallInt(index)
					values = values + strconv.FormatUint(v, 10) + ","
				} else {
					v, _ := smallInt(index)
					values = values + strconv.FormatInt(v, 10) + ","
				}
			}
			if columnIn.columnType == "INT" {
				if columnIn.unsigned {
					_, v := Int(index)
					values = values + strconv.FormatUint(v, 10) + ","
				} else {
					v, _ := Int(index)
					values = values + strconv.FormatInt(v, 10) + ","
				}
			}
			if columnIn.columnType == "BIGINT" {
				if columnIn.unsigned {
					_, v := bigInt(index)
					values = values + strconv.FormatUint(v, 10) + ","
				} else {
					v, _ := bigInt(index)
					values = values + strconv.FormatInt(v, 10) + ","
				}
			}
			if columnIn.columnType == "CHAR" {
				values = values + "'" + char(index) + "'" + ","
			}
			if columnIn.columnType == "VARCHAR" {
				values = values + "'" + varChar(index) + "'" + ","
			}
			if columnIn.columnType == "TIMESTAMP" {
				values = values + "'" + timeStamp(index) + "'" + ","
			}
			if columnIn.columnType == "DATETIME" {
				values = values + "'" + dateTime(index) + "'" + ","
			}
			if columnIn.columnType == "DATE" {
				values = values + "'" + date(index) + "'" + ","
			}
			if columnIn.columnType == "YEAR" {
				values = values + "'" + year(index) + "'" + ","
			}

		}
		strValues := values[:len(values)-1]
		InsertData(strValues)
		atomic.AddInt64(&dataCount, 1)
	}
	db.Close()
	ch <- 1

}
