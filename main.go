package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/tidwall/gjson"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	MySqlAdr     string `json:"my_sql_adr"`
	DatabaseName string `json:"database_name"`
	Account      string `json:"account"`
	Password     string `json:"password"`
	Page         int    `json:"page"`
}

var (
	config         Config
	tableName            = ""
	requestUrl           = ""
	dataSourceName       = ""
	page           int32 = -1
)

var httpclient = sync.Pool{
	New: func() interface{} {
		return &http.Client{}
	},
}

var DBpool = sync.Pool{
	New: func() interface{} {
		conn, err := sql.Open("mysql", dataSourceName)
		if err != nil {
			panic(err)
		}
		return conn
	},
}

func getClanRank() gjson.Result {
	client := httpclient.Get().(*http.Client)
	nowPage := atomic.AddInt32(&page, 1)
	url := requestUrl + "&page=" + strconv.Itoa(int(nowPage))
	fmt.Println("正在爬取第", nowPage, "页")
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return gjson.Result{}
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36")
	resp, err := client.Do(req)
	httpclient.Put(client)
	defer resp.Body.Close()
	if err != nil {
		return gjson.Result{}
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return gjson.Result{}
	}
	return gjson.Parse(string(body))
}

func updateClanRank() {
	ch := make(chan gjson.Result)
	requestUrl = "https://tools-wiki.biligame.com/pcr/getTableInfo?type=rank"

	con, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		panic(err)
	}

	_, err = con.Exec("create table " + tableName +
		"(" +
		"rank int(10)," +
		"damage bigint(20)," +
		"clan_name varchar(40)," +
		"leader_name varchar(40)" +
		")")
	if err != nil {
		fmt.Println(err)
	}
	DBpool.Put(con)
	insertSql := "insert into " + tableName + " (rank, damage, clan_name, leader_name) values (?,?,?,?)"
	go func() {
		for rawData := range ch {
			go func(data gjson.Result) {
				fmt.Println("正在上传中")
				conn := DBpool.Get().(*sql.DB)
				data.ForEach(func(key, value gjson.Result) bool {
					rank := value.Get("rank").Int()
					damage := value.Get("damage").Int()
					clanName := value.Get("clan_name").Str
					leaderName := value.Get("leader_name").Str
					_, err := conn.Exec(insertSql, rank, damage, clanName, leaderName)
					if err != nil {
						fmt.Println(err)
					}
					return true
				})
				DBpool.Put(conn)
			}(rawData)

		}
	}()

	for i := 0; i < config.Page; i++ {
		time.Sleep(60 * time.Millisecond)
		go func() {
			ch <- getClanRank()
		}()
	}
}

func main() {
	start := time.Now()
	d, err := ioutil.ReadFile("config.json")
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(d, &config)
	if err != nil {
		panic(err)
	}
	dataSourceName = fmt.Sprintf("%v:%v@tcp(%v)/%v?charset=utf8mb4",
		config.Account,
		config.Password,
		config.MySqlAdr,
		config.DatabaseName,
	)
	tableName = fmt.Sprintf("clanrank_%v_%v_%v_%v", start.Month(), start.Day(), start.Hour(),start.Minute())
	updateClanRank()
	fmt.Println("用时：", time.Since(start))
}
