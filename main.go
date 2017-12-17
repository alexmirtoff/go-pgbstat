/*

*** (c) 2017-2018 by Alex Mirtoff
*** alex@mirtoff.ru
*** telegram: mirt0ff

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
    //. "github.com/blacked/go-zabbix"
)


type PgbouncerPools struct {
    database     string
    user         string
    cl_active	 int
    cl_waiting 	 int
    sv_active	 int
    sv_idle	 int
    sv_used	 int
    sv_tested    int
    sv_login     int
    maxwait      int
    pool_mode    string
}

type PgbouncerDatabases struct {
    name	  string
    host	  sql.NullString
    force_user	  string
    port	  int
    database	  string
    pool_size     int
    reserve_pool  int
    pool_mode     string
    max_conn	  int
    curr_conn	  int
}

type PgbouncerClients struct {
    cl_type	  string
    user	  string
    database	  string
    state	  string
    addr	  string
    port          int
    local_addr    string
    local_port	  int
    connect_time  string
    request_time  string
    ptr		  string
    link	  string
    remote_pid    int
    tls		  string
}

type PgbouncerServers struct {
    sv_type	  string
    user	  string
    database	  string
    state	  string
    addr	  string
    port	  int
    local_addr	  string
    local_port    int
    connect_time  string
    request_time  string
    ptr		  string
    link	  string
    remote_pid    string
    tls		  string
}

type PgbouncerStats struct {
    database	  string
    total_req     int
    total_rec     int
    total_sent    int
    total_q_time  int
    avg_req	  int
    avg_recv	  int
    avg_sent      int
    avg_query	  int 
}

type PgbouncerLists struct {
    list	  string
    items	  string
}


// start here
func main() {
    conf := initConfig()
    

    args := os.Args[1:]
    if len(args) < 1 {
	fmt.Print("go-pgbstat version 0.1\n\nUsage: go-pgbstat [-dv]\n")
        fmt.Print("-d daemonize\n-v version\n\n")
        os.Exit(0)
    } else if os.Args[1] == "-v" {
        fmt.Print("go-pgbstat version 0.1\n\u00a9 2017-2018 by Alex Mirtoff\ne-mail: amirtov@alfabank.ru\nhttps://github.com/alexmirtoff\n\n")
	os.Exit(0)
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
    _, err = conf.String("pgbouncer.password")
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
    
    /*
    zabbixHost, err := conf.String("zabbix.hostname")
    if err != nil {
        log.Fatal(err)
    }
    zabbixPort, err := conf.String("zabbix.port")
    if err != nil {
        log.Fatal(err)
    }
    */

     // Create a new HTTP client
    inflxConnect, err := client.NewHTTPClient(client.HTTPConfig{
                  Addr:         inflxHost,
                  Username:     inflxUser,
                  Password:     inflxPwd,
    })
    if err != nil {
        log.Fatal(err)
    }

 
    for {

    sqlParams := fmt.Sprintf("host=%s user=%s port=%s dbname=%s sslmode=disable", bhost, busername, bport, bdatabase) 
    db, err := sql.Open("postgres", sqlParams)
    if err != nil {
        log.Fatal(err)
    }
    

    poolsResult, err := getBouncerPoolData(db)
    databasesResult, err := getBouncerDatabaseData(db)
    clientsResult, err := getBouncerClientData(db)
    serversResult, err := getBouncerServerData(db)
    statsResult, err := getBouncerStatData(db)
    listsResult, err := getBouncerListData(db)

    db.Close()

    poolsMap := make(map[string]interface{})
    dbMap    := make(map[string]interface{})
    clMap    := make(map[string]interface{})
    svMap    := make(map[string]interface{})
    statsMap := make(map[string]interface{})
    listsMap := make(map[string]interface{})


    for _, pgLists := range poolsResult {
        poolsMap["database"]   = pgLists.database
	poolsMap["user"]       = pgLists.user
        poolsMap["cl_active"]  = pgLists.cl_active
	poolsMap["cl_waiting"] = pgLists.cl_waiting
	poolsMap["sv_active"]  = pgLists.sv_active
	poolsMap["sv_idle"]    = pgLists.sv_idle
	poolsMap["sv_used"]    = pgLists.sv_used
	poolsMap["sv_tested"]  = pgLists.sv_tested
	poolsMap["sv_login"]   = pgLists.sv_login
	poolsMap["maxwait"]    = pgLists.maxwait
	poolsMap["pool_mode"]  = pgLists.pool_mode
    }  
    for _, pgLists := range databasesResult {
        dbMap["name"]          = pgLists.name
	dbMap["host"]          = pgLists.host
	dbMap["port"]          = pgLists.port
	dbMap["database"]      = pgLists.database
	dbMap["force_user"]    = pgLists.force_user
	dbMap["pool_size"]     = pgLists.pool_size
	dbMap["reserve_pool"]  = pgLists.reserve_pool
	dbMap["pool_mode"]     = pgLists.pool_mode
	dbMap["max_conn"]      = pgLists.max_conn
	dbMap["curr_conn"]     = pgLists.curr_conn
    }
    for _, pgLists := range clientsResult {
        clMap["type"]          = pgLists.cl_type
	clMap["user"]          = pgLists.user
	clMap["database"]      = pgLists.database
	clMap["state"]         = pgLists.state
	clMap["addr"]          = pgLists.addr
	clMap["port"]          = pgLists.port
	clMap["local_addr"]    = pgLists.local_addr
	clMap["local_port"]    = pgLists.local_port
	clMap["connect_time"]  = pgLists.connect_time
	clMap["request_time"]  = pgLists.request_time
	clMap["ptr"]           = pgLists.ptr
	clMap["link"]          = pgLists.link
	clMap["remote_pid"]    = pgLists.remote_pid
	clMap["tls"]           = pgLists.tls
    }
    for _, pgLists := range serversResult {
        svMap["type"]          = pgLists.sv_type
        svMap["user"]          = pgLists.user
        svMap["database"]      = pgLists.database
        svMap["state"]         = pgLists.state
        svMap["addr"]          = pgLists.addr
        svMap["port"]          = pgLists.port
        svMap["local_addr"]    = pgLists.local_addr
        svMap["local_port"]    = pgLists.local_port
        svMap["connect_time"]  = pgLists.connect_time
        svMap["request_time"]  = pgLists.request_time
        svMap["ptr"]           = pgLists.ptr
        svMap["link"]          = pgLists.link
        svMap["remote_pid"]    = pgLists.remote_pid
        svMap["tls"]           = pgLists.tls
    }
    for _, pgLists := range statsResult {
        statsMap["database"]     = pgLists.database
	statsMap["total_req"]    = pgLists.total_req
	statsMap["total_rec"]    = pgLists.total_rec
	statsMap["total_sent"]   = pgLists.total_sent
	statsMap["total_q_time"] = pgLists.total_q_time
	statsMap["avg_req"]      = pgLists.avg_req
	statsMap["avg_recv"]     = pgLists.avg_recv
	statsMap["avg_sent"]     = pgLists.avg_sent
	statsMap["avg_query"]    = pgLists.avg_query
    }
    for _, pgLists := range listsResult {
        listsMap[pgLists.list] = pgLists.items
    }

    // check empty maps
    if len(poolsMap) > 0 {
        bpPools, err := createPointBatch(inflxDb, "m_pools", "user", poolsMap)
        writeBatch(inflxConnect, bpPools)
        if err != nil {
	    log.Fatal(err)
        }
    }
    if len(dbMap) > 0 {
        bpDb, err    := createPointBatch(inflxDb, "m_databases", "db", dbMap)
        writeBatch(inflxConnect, bpDb)
        if err != nil {
            log.Fatal(err)
        }
    }
    if len(clMap) > 0 {
        bpCl, err    := createPointBatch(inflxDb, "m_clients", "client", clMap)
        writeBatch(inflxConnect, bpCl)
        if err != nil {
            log.Fatal(err)
        }
    }
    if len(svMap) > 0 {
    
        bpSv, err   := createPointBatch(inflxDb, "m_servers", "server", svMap)
        writeBatch(inflxConnect, bpSv)
        if err != nil {
	    log.Fatal(err)
        }
    }
    if len(statsMap) > 0 {
        bpSt, err    := createPointBatch(inflxDb, "m_stats", "stats", statsMap)
        writeBatch(inflxConnect, bpSt)
        if err != nil {
            log.Fatal(err)
        }
    }
    if len(listsMap) > 0 {
        bpLs, err    := createPointBatch(inflxDb, "m_lists", "lists", listsMap)
        writeBatch(inflxConnect, bpLs)
        if err != nil {
            log.Fatal(err)
        }
    }
   time.Sleep(5000 * time.Millisecond)
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


// getting POOLS data
func getBouncerPoolData(db *sql.DB) (Pools[]*PgbouncerPools, err error) {

    rows, err := db.Query("SHOW POOLS")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    result := make([]*PgbouncerPools, 0)
    for rows.Next() {
        pgPools := new(PgbouncerPools)
        err := rows.Scan(&pgPools.database, &pgPools.user, &pgPools.cl_active, &pgPools.cl_waiting, &pgPools.sv_active,
                         &pgPools.sv_idle, &pgPools.sv_used, &pgPools.sv_tested, &pgPools.sv_login, &pgPools.maxwait, &pgPools.pool_mode)
        if err != nil {
            log.Fatal(err)
        }
        result = append(result, pgPools)
    }
    if err = rows.Err(); err != nil {
        log.Fatal(err)
    }
    return result, err

}

// getting DATABASES data
func getBouncerDatabaseData(db *sql.DB) (Databases[]*PgbouncerDatabases, err error) {

    rows, err := db.Query("SHOW DATABASES")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    result := make([]*PgbouncerDatabases, 0)
    for rows.Next() {
        pgDatabases := new(PgbouncerDatabases)
        err := rows.Scan(&pgDatabases.name, &pgDatabases.host, &pgDatabases.port, &pgDatabases.database, &pgDatabases.force_user,
                         &pgDatabases.pool_size, &pgDatabases.reserve_pool, &pgDatabases.pool_mode, &pgDatabases.max_conn, &pgDatabases.curr_conn)
        if err != nil {
            log.Fatal(err)
        }
	result = append(result, pgDatabases)
    }
    if err = rows.Err(); err != nil {
        log.Fatal(err)
    }
    return result, err
}

// getting CLIENTS data
func getBouncerClientData(db *sql.DB) (Clients[]*PgbouncerClients, err error) {

    rows, err := db.Query("SHOW CLIENTS")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    result := make([]*PgbouncerClients, 0)
    for rows.Next() {
        pgClients := new(PgbouncerClients)
        err := rows.Scan(&pgClients.cl_type, &pgClients.user, &pgClients.database, &pgClients.state, &pgClients.addr, &pgClients.port,
                         &pgClients.local_addr, &pgClients.local_port, &pgClients.connect_time, &pgClients.request_time, &pgClients.ptr,
                         &pgClients.link, &pgClients.remote_pid, &pgClients.tls)
        if err != nil {
            log.Fatal(err)
        }
        result = append(result, pgClients)
    }
    if err = rows.Err(); err != nil {
        log.Fatal(err)
    }
    return result, err
}

// getting SERVERS data
func getBouncerServerData(db *sql.DB) (Servers[]*PgbouncerServers, err error) {

    rows, err := db.Query("SHOW SERVERS")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    result := make([]*PgbouncerServers, 0)
    for rows.Next() {
        pgServers := new(PgbouncerServers)
        err := rows.Scan(&pgServers.sv_type, &pgServers.user, &pgServers.database, &pgServers.state, &pgServers.addr, &pgServers.port,
			 &pgServers.local_addr, &pgServers.local_port, &pgServers.connect_time, &pgServers.request_time, &pgServers.ptr,
		         &pgServers.link, &pgServers.remote_pid, &pgServers.tls)
        if err != nil {
            log.Fatal(err)
        }
        result = append(result, pgServers)
    }
    if err = rows.Err(); err != nil {
        log.Fatal(err)
    }
    return result, err
}

// getting STATS data
func getBouncerStatData(db *sql.DB) (Stats[]*PgbouncerStats, err error) {

    rows, err := db.Query("SHOW STATS")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    result := make([]*PgbouncerStats, 0)
    for rows.Next() {
        pgStats := new(PgbouncerStats)
        err := rows.Scan(&pgStats.database, &pgStats.total_req, &pgStats.total_rec, &pgStats.total_sent, &pgStats.total_q_time,
		         &pgStats.avg_req, &pgStats.avg_recv, &pgStats.avg_sent, &pgStats.avg_query)
        if err != nil {
            log.Fatal(err)
        }
        result = append(result, pgStats)
    }
    if err = rows.Err(); err != nil {
        log.Fatal(err)
    }
    return result, err
}

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
func createPointBatch(inflxDb string, measurement string, t string, fields map[string]interface{}) (bp client.BatchPoints, err error){

    // Create a new point batch
    bp, err = client.NewBatchPoints(client.BatchPointsConfig{
                  Database:     inflxDb,
                  Precision:    "s",
    })

    // Construct tags and fields here
    tags := map[string]string{"type": t}

    //loc, err := time.LoadLocation("Europe/Moscow")
    //if err != nil {
    //	log.Fatal(err)
    //  }
    //tn := time.Now().In(loc)

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
