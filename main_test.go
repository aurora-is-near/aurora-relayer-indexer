package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func Test_PanicWithWrongDB(t *testing.T) {
	fromBlock = 60034225
	toBlock = 60034226
	sourceFolder="./fixtures"
	databaseURL = "postgres://wrong_db"

	assert.Panics(t, func() {
		rootCmd.Execute()
	}, "The code did not panic because of wrong DB")
}


func Test_IndexFailedBlocks(t *testing.T) {
	database := prepareDatabase()
	defer database.Close()

	databaseURL = getDatabaseURL()
	fromBlock = 50000000
	toBlock = 50000001
	keepFiles = true
	sourceFolder="./fixtures"

	go func() {
			rootCmd.Execute()
	}()

	<-time.After(2 * time.Second)

	t.Run("block not created", func(t *testing.T) {
		blockCount, _ := entriesCount(database, "block")
		if blockCount != 0 {
			t.Errorf("Block inserted")
		}
	})

	t.Run("transactions not created", func(t *testing.T) {
		transactionCount, err := entriesCount(database, "transaction")
		if err != nil {
			t.Errorf(err.Error())
		}
		if transactionCount != 0 {
			t.Errorf("Transaction inserted")
		}
	})

	t.Run("events not created", func(t *testing.T) {
		eventCount, err := entriesCount(database, "event")
		if err != nil {
			t.Errorf(err.Error())
		}
		if eventCount != 0 {
			t.Errorf("Event inserted")
		}
	})
}

func Test_IndexBlocks(t *testing.T) {
	database := prepareDatabase()
	defer database.Close()

	databaseURL = getDatabaseURL()
	fromBlock = 60034225
	toBlock = 60034226
	keepFiles = true
	sourceFolder="./fixtures"

	rootCmd.Execute()

	t.Run("block created", func(t *testing.T) {
		blockCount, _ := entriesCount(database, "block")
		if blockCount != 1 {
			t.Errorf("Block not inserted")
		}
	})

	t.Run("transactions created", func(t *testing.T) {
		transactionCount, err := entriesCount(database, "transaction")
		if err != nil {
			t.Errorf(err.Error())
		}
		if transactionCount != 3 {
			t.Errorf("Transaction not inserted")
		}
	})

	t.Run("events created", func(t *testing.T) {
		eventCount, err := entriesCount(database, "event")
		if err != nil {
			t.Errorf(err.Error())
		}
		if eventCount != 17 {
			t.Errorf("Event not inserted")
		}
	})
}

func Test_IndexBlocksFromBlockLessGenesisBlock(t *testing.T) {
	database := prepareDatabase()
	defer database.Close()

	databaseURL = getDatabaseURL()
	fromBlock = 0
	toBlock = 9820211
	genesisBlock = 9820210
	keepFiles = true
	sourceFolder="./fixtures"

	rootCmd.Execute()

	t.Run("block created", func(t *testing.T) {
		blockCount, _ := entriesCount(database, "block")
		if blockCount != 1 {
			t.Errorf("Block not inserted")
		}
	})
}

func Test_IndexBlocksLargeCase(t *testing.T) {
	database := prepareDatabase()
	defer database.Close()

	databaseURL = getDatabaseURL()
	fromBlock = 73097407
	toBlock = 73097408
	keepFiles = true
	sourceFolder="./fixtures"

	rootCmd.Execute()

	t.Run("block created", func(t *testing.T) {
		blockCount, _ := entriesCount(database, "block")
		if blockCount != 1 {
			t.Errorf("Block not inserted")
		}
	})

	t.Run("transactions created", func(t *testing.T) {
		transactionCount, err := entriesCount(database, "transaction")
		if err != nil {
			t.Errorf(err.Error())
		}
		if transactionCount != 2 {
			t.Errorf("Transaction not inserted")
		}
	})

	t.Run("transaction huge input inserted", func(t *testing.T) {
		var input string
		database.QueryRow(context.Background(), "SELECT input::varchar FROM transaction WHERE index = 0 LIMIT 1").Scan(&input)
		if len(input) != 1126340 {
			t.Errorf("Input size dows not match")
		}
	})
}

func prepareDatabase() *pgxpool.Pool {
	viper.AddConfigPath("config")
	viper.SetConfigName("test")
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}
	database, _ := pgxpool.Connect(context.Background(), viper.GetString("database"))
	_, _ = database.Exec(context.Background(), "TRUNCATE block, event, transaction")
	return database
}

func getDatabaseURL() string {
	viper.AddConfigPath("config")
	viper.SetConfigName("test")
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}
	databaseURL := viper.GetString("database")
	return databaseURL
}

func entriesCount(database *pgxpool.Pool, table string) (int, error) {
	var amount int
	err := database.QueryRow(context.Background(), fmt.Sprintf("SELECT COUNT(1) FROM %s", table)).Scan(&amount)
	if err != nil {
		return 0, err
	}
	return amount, nil
}
