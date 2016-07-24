#dbsl

dbsl is a simple tool for generating data with golang.through parse TOML and generate test data ,connect to Mysql and insert data into database.



#How to use 

 - edit config.toml
 - go run
 
#Example
	title="db.config"
	[mysql]

	#set connect ip and port.
	ip="localhost"
	port="3306"

	#set database
	database="dbsl"

	#set table name
	tablename="match_players"

	#set username and password
	username="root"
	password="abcd"

	[sql]
	#set create table statement
	tablestmt="CREATE TABLE match_players (match_id bigint,match_seq_num bigint,);"
	#set create index statement
	indexstmt=""

	[goroutine]
	#set goruntine number
	goroutinenum=10

	[Data]
	datanumber=1000
 
#Lincense
MIT