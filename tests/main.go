package main

import (
	"fmt"
	"log"
	"os"

	"github.com/naipad/loach"
)

func main() {
	os.RemoveAll("./data")
	os.RemoveAll("./backup")
	db, err := loach.OpenDefault("./data")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	testData := map[string]string{
		"user:1": "Alice",
		"user:2": "Bob",
		"user:3": "Charlie",
	}

	for k, v := range testData {
		err := db.Put([]byte(k), []byte(v), nil)
		if err != nil {
			log.Fatalf("Failed to insert test data: %v", err)
		}
	}
	log.Println("Test data inserted")

	db.Compact()

	stats, err := db.StatsStruct()
	if err != nil {
		log.Fatalf("Failed to get DB stats: %v", err)
	}

	fmt.Println("Compaction stats:\n", stats.Compactions)

	for level, tableStat := range stats.TableStats {
		fmt.Printf("Level %d: %d tables, %.2f MB, score %.2f\n", level, tableStat.Tables, tableStat.SizeMB, tableStat.Score)
	}

	for level, sstables := range stats.SSTables {
		fmt.Printf("Level %d SSTables:\n", level)
		for _, entry := range sstables {
			fmt.Printf("  File #%d: Size %d bytes\n", entry.FileNumber, entry.FileSize)
		}
	}

	// Perform backup
	err = db.BackupTo("./backup")
	if err != nil {
		log.Println("Backup failed:", err)
	} else {
		log.Println("Backup successful")
	}

	// Delete original data to simulate loss
	for k := range testData {
		_ = db.Delete([]byte(k), nil)
	}
	log.Println("Original data deleted")

	// Verify deletion
	for k := range testData {
		val, err := db.Get([]byte(k), nil)
		if err == nil && val != nil {
			log.Fatalf("Data was not deleted: %s => %s", k, string(val))
		}
	}
	log.Println("Deletion verified")

	// Perform restore
	err = db.RestoreFrom("./backup")
	if err != nil {
		log.Println("Restore failed:", err)
	} else {
		log.Println("Restore successful")
	}

	// Verify restored data
	for k, v := range testData {
		val, err := db.Get([]byte(k), nil)
		if err != nil {
			log.Fatalf("Failed to read restored data: %s", k)
		}
		if string(val) != v {
			log.Fatalf("Data mismatch: %s => %s (expected: %s)", k, val, v)
		}
		fmt.Printf("Verified: %s => %s\n", k, val)
	}

	db.Compact()

	stats, err = db.StatsStruct()
	if err != nil {
		log.Fatalf("Failed to get DB stats: %v", err)
	}

	fmt.Println(stats.Compactions)

	for level, tableStat := range stats.TableStats {
		fmt.Printf("Level %d: %d tables, %.2f MB, score %.2f\n", level, tableStat.Tables, tableStat.SizeMB, tableStat.Score)
	}

	for level, sstables := range stats.SSTables {
		fmt.Printf("Level %d SSTables:\n", level)
		for _, entry := range sstables {
			fmt.Printf("  File #%d: Size %d bytes\n", entry.FileNumber, entry.FileSize)
		}
	}

	db.Close()
	// Clean up
	os.RemoveAll("./data")
	os.RemoveAll("./backup")
	log.Println("Test completed, temporary data removed")
}
