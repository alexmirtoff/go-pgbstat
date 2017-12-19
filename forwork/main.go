/*

*** (c) 2017-2018 by Alex Mirtoff
*** alex@mirtoff.ru
*** telegram: @mirt0ff
*** version 0.1.2

Getting statistics data from pgBouncer by sql connection and inserting it into
InfluxDB or Zabbix. Single database & user.

*/

package main

import (
    "fmt"
    "os"
    "time"
    "log"
    _ "github.com/lib/pq"
    "database/sql"
    "github.com/zpatrick/go-config"
    "github.com/influxdata/influxdb/client/v2"
    . "github.com/blacked/go-zabbix"
)


type PgbouncerLists struct {
    list	  string
    items	  string
}

// start here
func main() {
    conf := initConfig()
    

    args := os.Args[1:]
    if len(args) < 1 {
	fmt.Print("go-pgbstat version 0.1.2\n\nUsage: go-pgbstat [-dv]\n")
        fmt.Print("-d daemonize\n-v version\n\n")
        os.Exit(0)
    } else if os.Args[1] == "-v" {
        fmt.Print("go-pgbstat version 0.1.2\n\u00a9 2017-2018 by Alex Mirtoff\ne-mail: amirtov@alfabank.ru\nhttps://github.com/alexmirtoff\n\n")
	os.Exit(0)
    } 
    
    // Global settings init
    sendInt, err := conf.Int("global.send_interval")
    if err != nil {
        log.Fatal(err)
    }
    senderMethod, err := conf.String("global.sender")
    if err != nil {
        log.Fatal(err)
    }
    

    // pgbouncer settings init
    bhost, err := conf.String("pgbouncer.hostname")
    if err != nil {
        log.Fatal(err)
    }
    bdatabase, err := conf.String("pgbouncer.database")
    if err != nil {
        log.Fatal(err)
    }

    bport, err := conf.String("pgbouncer.port")
    if err != nil {
        log.Fatal(err)
    }

    busername, err := conf.String("pgbouncer.username")
    if err != nil {
        log.Fatal(err)
    }
    bpassword, err := conf.String("pgbouncer.password")
    if err != nil {
        log.Fatal(err)
    }

    // Influx settings init
    inflxHost, err := conf.String("influxdb.hostname")
    if err != nil { 
        log.Fatal(err)
    } 
    inflxPort, err := conf.String("influxdb.port")
    if err != nil {
        log.Fatal(err)
    }

    inflxHost = fmt.Sprintf("http://%s:%s", inflxHost, inflxPort)
    inflxDb, err := conf.String("influxdb.database")
    if err != nil {
        log.Fatal(err)
    }
    inflxUser, err := conf.String("influxdb.username")
    if err != nil {
	log.Fatal(err)
    }
    inflxPwd, err := conf.String("influxdb.password")
    if err != nil {
        log.Fatal(err)
    }
    
    
    zabbixHost, err := conf.String("zabbix.hostname")
    if err != nil {
        log.Fatal(err)
    }
    zabbixPort, err := conf.Int("zabbix.port")
    if err != nil {
        log.Fatal(err)
    }
    zabbixAgentHost, err := conf.String("zabbix.agent_host")
    if err != nil {
        log.Fatal(err)
    }

     //Create a new HTTP client
      inflxConnect, err := client.NewHTTPClient(client.HTTPConfig{
                    Addr:         inflxHost,
                    Username:     inflxUser,
                    Password:     inflxPwd,
      })
      if err != nil {
          log.Fatal(err)
      }

    for {

    sqlParams := fmt.Sprintf("host=%s user=%s password=%s port=%s dbname=%s sslmode=disable", bhost, busername, bpassword, bport, bdatabase) 
    db, err := sql.Open("postgres", sqlParams)
    if err != nil {
        log.Fatal(err)
    }
    

    listsResult, err := getBouncerListData(db)

    db.Close()

    listsMap := make(map[string]interface{})
    listsTags := make(map[string]string)

    for _, pgLists := range listsResult {
        listsMap[pgLists.list] = pgLists.items
	
    }
    // check empty maps and construct batch DATA (conn name, measurement, tags, fields) 


    if len(listsMap) > 0 && senderMethod == "influx" {
        bpLs, err    := createPointBatch(inflxDb, "m_lists", listsTags, listsMap)
        writeBatch(inflxConnect, bpLs)
        if err != nil {
            log.Fatal(err)
        }
    }

   
    // Send to Zabbix
    if senderMethod == "zabbix" {
        sendToZabbix(zabbixHost, zabbixPort, zabbixAgentHost,listsMap)
    }

   time.Sleep(time.Duration(sendInt) * time.Millisecond)
   }
  
}

// init config.ini
func initConfig() *config.Config {
    iniFile := config.NewINIFile("config.ini")
    return config.NewConfig([]config.Provider{iniFile})
}

/*

PgBouncer Section

*/

// getting LISTS data
func getBouncerListData(db *sql.DB) (Lists[]*PgbouncerLists, err error) {

    rows, err := db.Query("SHOW LISTS")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    result := make([]*PgbouncerLists, 0)
    for rows.Next() {
        pgLists := new(PgbouncerLists)
        err := rows.Scan(&pgLists.list, &pgLists.items)
        if err != nil {
            log.Fatal(err)
        }
        result = append(result, pgLists)
    }
    if err = rows.Err(); err != nil {
        log.Fatal(err)
    }
    return result, err
}

/*

InfluxDB Section

*/

// Batch Points Constructor
func createPointBatch(inflxDb string, measurement string, tags map[string]string, fields map[string]interface{}) (bp client.BatchPoints, err error){

    // Create a new point batch
    bp, err = client.NewBatchPoints(client.BatchPointsConfig{
                  Database:     inflxDb,
                  Precision:    "s",
    })

    pt, err := client.NewPoint(measurement, tags, fields, time.Now())
    if err != nil {
       log.Fatal(err)
    }
    bp.AddPoint(pt)
    return bp, err

}

// Write Batch
func writeBatch(inflxConnect client.Client, BPoint client.BatchPoints) {

 if err := inflxConnect.Write(BPoint); err != nil {
        log.Fatal(err)
 }
 inflxConnect.Close()

}

/*

Zabbix Section

*/

func sendToZabbix(zabbixHost string, zabbixPort int, zabbixAgentHost string, lists map[string]interface{}) {
    var metrics []*Metric

    for k, v := range lists {
        k = fmt.Sprintf("bouncerStat[%s]", k)
        metrics = append(metrics, NewMetric(zabbixAgentHost, k, v.(string)))
    }
    
    packet := NewPacket(metrics)

    z := NewSender(zabbixHost, zabbixPort)
    z.Send(packet)
}

