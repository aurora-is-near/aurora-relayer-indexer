package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var configFile, databaseURL, sourceFolder string
var debug, keepFiles bool
var fromBlock, toBlock, genesisBlock uint64

func main() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "", "Config file (default is config/local.yaml)")
	rootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "Enable debugging(default is false)")
	rootCmd.PersistentFlags().BoolVarP(&keepFiles, "keepFiles", "k", false, "Keep json files(default is false)")
	rootCmd.PersistentFlags().StringVarP(&databaseURL, "database", "u", "", "database url (default is postgres://aurora:aurora@database/aurora)")
	rootCmd.PersistentFlags().StringVarP(&sourceFolder, "sourceFolder", "s", "../borealis-engine-lib/output/refiner", "Source folder populated with block.json files (default is ../borealis-engine-lib/output/refiner)")
	rootCmd.PersistentFlags().Uint64VarP(&fromBlock, "fromBlock", "f", 0, "Block to start from. Ignored if missing or 0 (default is 0)")
	rootCmd.PersistentFlags().Uint64VarP(&toBlock, "toBlock", "t", 0, "Block to end on. Ignored if missing or 0 (default is 0)")
	rootCmd.PersistentFlags().Uint64VarP(&genesisBlock, "genesisBlock", "g", 1, "Aurora genesis block. It will never index blocks prior to it. (default is 1)")
	cobra.CheckErr(rootCmd.Execute())
}

func initConfig() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	if configFile != "" {
		log.Warn().Msg(fmt.Sprint("Using config file:", viper.ConfigFileUsed()))
		viper.SetConfigFile(configFile)
	} else {
		viper.AddConfigPath("config")
		viper.AddConfigPath("../../config")
		viper.SetConfigName("local")
		if err := viper.BindPFlags(rootCmd.PersistentFlags()); err != nil {
			panic(fmt.Errorf("Flags are not bindable: %v\n", err))
		}
	}
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err == nil {
		log.Warn().Msg(fmt.Sprint("Using config file:", viper.ConfigFileUsed()))
	}
	debug = viper.GetBool("debug")
	databaseURL = viper.GetString("database")
	sourceFolder = viper.GetString("sourceFolder")
	fromBlock = viper.GetUint64("fromBlock")
	toBlock = viper.GetUint64("toBlock")
	genesisBlock = viper.GetUint64("genesisBlock")
}

var rootCmd = &cobra.Command{
	Use:     "indexer",
	Short:   "Consumes json files to produce blocks.",
	Long:    `Consumes json files to produce blocks. Generates sql and iserts them to db.`,
	Version: "0.0.1",
	Run: func(cmd *cobra.Command, args []string) {
		pgpool, err := pgxpool.Connect(context.Background(), databaseURL)
		if err != nil {
			panic(fmt.Errorf("Unable to connect to database %s: %v\n", databaseURL, err))
		}
		defer pgpool.Close()

		if fromBlock == 0 {
			fromBlock, err = getPendingBlockHeight(pgpool)
			if err != nil {
				panic(fmt.Errorf("Can not retreive last indexed block from db: %v\n", err))
			}
		}

		if fromBlock < genesisBlock {
			fromBlock = genesisBlock
		}

		go indexBlocks(sourceFolder, pgpool, Uint64(fromBlock), Uint64(toBlock))

		interrupt := make(chan os.Signal, 10)
		signal.Notify(interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT)

		<-interrupt
		os.Exit(0)
	},
}

func indexBlocks(folder string, pgpool *pgxpool.Pool, fromBlock Uint64, toBlock Uint64) {
	for {
		subFolder := getSubFolder(folder, fromBlock)
		fileName := fmt.Sprintf("%s/%v.json", subFolder, fromBlock)
		content, err := os.ReadFile(fileName)
		if err != nil {
			log.Debug().Msg(fmt.Sprintf("Waiting for new block in %v..", fileName))
			wait()
		} else {
			var block Block
			err := json.Unmarshal([]byte(content), &block)
			if err != nil {
				log.Warn().Msg(fmt.Sprintf("Failed to parse: %+v. Retrying..\n", err))
				wait()
			} else {
				block.Sequence = block.Height
				sql := block.insertSql()
				// fmt.Println(sql)
				_, err := pgpool.Exec(context.Background(), sql)

				if err != nil {
					log.Warn().Msg(fmt.Sprintf("Unable import block %v: %v\n", block.Height, err))
					wait()
				} else {
					log.Info().Msg(fmt.Sprintf("%+v", block.Height))
					if !keepFiles {
						cleanup(fileName, folder, block.Height)
					}
				}
				fromBlock += 1
			}
		}
	}
}

func getPendingBlockHeight(pgpool *pgxpool.Pool) (uint64, error) {
	var blockID uint64
	err := pgpool.QueryRow(context.Background(),
		"SELECT COALESCE(MAX(id), 0) FROM block").Scan(&blockID)
	if err != nil {
		return 0, err
	}
	return blockID + 1, nil
}

func cleanup(fileName string, folder string, block Uint64) {
	err := os.Remove(fileName)
	if err != nil {
		log.Warn().Msg(fmt.Sprintf("Unable to remove file %v: %v\n", fileName, err))
	}
	if block == block/10000*10000 {
		subFolder := getSubFolder(folder, block-1)
		err := os.Remove(subFolder)
		if err != nil {
			log.Warn().Msg(fmt.Sprintf("Unable to remove folder %v: %v\n", subFolder, err))
		}
	}
}

func wait() {
	time.Sleep(500 * time.Millisecond)
}

func getSubFolder(folder string, block Uint64) string {
	return fmt.Sprintf("%s/%v", folder, block/10000*10000)
}
