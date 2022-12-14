package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"time"

	"github.com/remeh/sizedwaitgroup"
	"github.com/spf13/cast"
)

func cdc() error {
	dsnCount := cast.ToInt64(os.Getenv("PG_DSN_COUNT"))
	if dsnCount < 1 {
		return fmt.Errorf("missing or invalid env var: pg_dsn_count")
	}

	for i := 1; i <= int(dsnCount); i++ {
		var dsnEnum int64 = int64(i)
		var nmsColumn string
		dbURL := os.Getenv("PG_DB_URL_" + cast.ToString(dsnEnum))
		nmsColumn = os.Getenv("PG_NMS_COLUMN_" + cast.ToString(dsnEnum))
		log.Printf("seed loop: %v\tdsnEnum %v\tdbURL %v\tnmsColumn %v\n", i, dsnEnum, dbURL, nmsColumn)
		pgPool, err := getPGConnection(dbURL)
		if err != nil {
			return fmt.Errorf("pg connection failure: %v", err)
		} else {
			log.Println("PG Connected")
		}
		nmsDB, err := nmsDBOpen()
		if err != nil {
			return fmt.Errorf("cdc nmsdbopen error: %v", err)
		}
		tables, err := nmsTablesQuery(nmsDB, false)
		if err != nil {
			return fmt.Errorf("cdc nmstablesquery error: %v", err)
		}
		batchCount := cast.ToFloat64(os.Getenv("BQ_BATCH_COUNT"))
		if batchCount == 0.0 {
			batchCount = 4096
		}
		replicationBufferSecs := cast.ToInt64(os.Getenv("PG_REPLICATION_BUFFER_SECS"))

		for i, t := range tables {
			var currentRowCount int64
			if t.DSNEnum == dsnEnum {
				currentRowCount, err = getTableRowCount(t.Name, pgPool)
				if err != nil {
					log.Printf("cdc gettablerowcount error: %v", err)
					continue
				}
			} else {
				continue
			}

			rowDiff := math.Abs(cast.ToFloat64(currentRowCount - t.LastRowCount))

			currentTime := time.Now()
			diff := currentTime.Sub(t.NMS)
			switch {
			case rowDiff > (batchCount * 8):
				hours := 336 // cast.ToInt64(diff.Hours() / (rowDiff / (batchCount * 8)))

				// set new nms at most x seconds in the past to account for replication delay if syncing from a replica
				if t.NMS.Add(time.Hour * time.Duration(hours)).Before(currentTime.Add(-time.Second * time.Duration(replicationBufferSecs))) {
					t.NewNMS = t.NMS.Add(time.Hour * time.Duration(hours))
				} else {
					t.NewNMS = currentTime.Add(-time.Second * time.Duration(replicationBufferSecs))
				}
				log.Printf("table %v.%v\t\t\trowDiff: %v\tdiff.Hours: %v\thours:%v\tnms:%v\tnewNMS: %v\n", t.DSNEnum, t.Name, rowDiff, math.Round(diff.Hours()), hours, t.NMS.Format("2006-01-02 15:04:05"), t.NewNMS.Format("2006-01-02 15:04:05"))
			case rowDiff == 0 && diff.Hours() > 336:
				hours := 336

				// set new nms at most x seconds in the past to account for replication delay if syncing from a replica
				if t.NMS.Add(time.Hour * time.Duration(hours)).Before(currentTime.Add(-time.Second * time.Duration(replicationBufferSecs))) {
					t.NewNMS = t.NMS.Add(time.Hour * time.Duration(hours))
				} else {
					t.NewNMS = currentTime.Add(-time.Second * time.Duration(replicationBufferSecs))
				}
				log.Printf("table %v.%v\t\t\trowDiff: %v\tdiff.Hours: %v\thours:%v\tnms:%v\tnewNMS: %v\n", t.DSNEnum, t.Name, rowDiff, math.Round(diff.Hours()), hours, t.NMS.Format("2006-01-02 15:04:05"), t.NewNMS.Format("2006-01-02 15:04:05"))
			case diff.Seconds() < cast.ToFloat64(replicationBufferSecs*2):
				t.NewNMS = t.NMS.Add(time.Second * cast.ToDuration(diff.Seconds()/3))
				log.Printf("table %v.%v\t\t\trowDiff: %v\tdiff.Hours: %v\t\tnms:%v\tnewNMS: %v\n", t.DSNEnum, t.Name, rowDiff, math.Round(diff.Hours()), t.NMS.Format("2006-01-02 15:04:05"), t.NewNMS.Format("2006-01-02 15:04:05"))
			default:
				t.NewNMS = currentTime.Add(-time.Second * time.Duration(replicationBufferSecs))
				log.Printf("table %v.%v\t\t\trowDiff: %v\tdiff.Hours: %v\t\tnms:%v\tnewNMS: %v\n", t.DSNEnum, t.Name, rowDiff, math.Round(diff.Hours()), t.NMS.Format("2006-01-02 15:04:05"), t.NewNMS.Format("2006-01-02 15:04:05"))

			}

			newNMS := t.NewNMS.Format("2006-01-02 15:04:05")
			tables[i].Query, err = getTableNMSQuery(t.Schema, t.Name, t.NMSColumn, t.NMS.Format("2006-01-02 15:04:05"), newNMS, pgPool)
			if err != nil {
				log.Printf("cdc gettablenmsquery error: %v", err)
				continue
			}
			tables[i].NewNMS = t.NewNMS
		}
		for i := range tables {
			if tables[i].Query != "" {
				tables[i].stream, err = newStream(dbURL, tables[i])
				if err != nil {
					return fmt.Errorf("cdc newstream error: %v", err)
				}
			}
		}
		// numCPUs := runtime.NumCPU()
		concurrentStreams := cast.ToInt(os.Getenv("BENTHOS_CONCURRENT_STREAMS"))
		if concurrentStreams > runtime.NumCPU() && runtime.NumCPU() > 1 {
			concurrentStreams = runtime.NumCPU() - 1
		}
		wg := sizedwaitgroup.New(concurrentStreams)

		for i := range tables {
			if tables[i].stream != nil {
				wg.Add()
				go func() {
					log.Printf("stream table %v.%v\n", tables[i].DSNEnum, tables[i].Name)
					defer wg.Done()
					err := tables[i].stream.Run(context.Background())
					if err != nil {
						log.Printf("stream failure: %v.%v - %v", tables[i].DSNEnum, tables[i].Name, err)
						return
					}
					err = updateNMS(tables[i], nmsDB)
					if err != nil {
						log.Printf("nms update error: id:%v - %v", tables[i].ID, err)
						return
					}
				}()
				// ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*60))
				// defer cancel()
				/*
					err := tables[i].stream.Run(context.Background())
					if err != nil {
						return fmt.Errorf("stream error: %v", err)
					}
					err = updateNMS(tables[i], nmsDB)
					if err != nil {
						return fmt.Errorf("nms update error: %v", err)
					}
				*/
			}
		}

		nmsDB.Close()
		pgPool.Close()
	}
	return nil
}
