package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	sqlblock "github.com/aurora-is-near/aurora-relayer-sqlblock"
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
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "", "config file (default is config/local.yaml)")
	rootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "enable debugging(default false)")
	rootCmd.PersistentFlags().BoolVarP(&keepFiles, "keepFiles", "k", false, "keep json files(default false)")
	rootCmd.PersistentFlags().StringVar(&databaseURL, "database", "", "database url (default postgres://aurora:aurora@database/aurora)")
	rootCmd.PersistentFlags().StringVarP(&sourceFolder, "sourceFolder", "s", "../borealis-engine-lib/output/refiner", "source folder populated with block.json files.")
	rootCmd.PersistentFlags().Uint64VarP(&fromBlock, "fromBlock", "f", 0, "block to start from. Ignored if missing or 0. (default 0)")
	rootCmd.PersistentFlags().Uint64VarP(&toBlock, "toBlock", "t", 0, "block to end on. Ignored if missing or 0. (default 0)")
	rootCmd.PersistentFlags().Uint64VarP(&genesisBlock, "genesisBlock", "g", 1, "aurora genesis block. It will never index blocks prior to it.")
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

		updateRefinerLastBlock(sourceFolder, fromBlock)

		go indexBlocks(sourceFolder, pgpool, fromBlock, toBlock)

		interrupt := make(chan os.Signal, 10)
		signal.Notify(interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT)

		<-interrupt
		os.Exit(0)
	},
}

func indexBlocks(folder string, pgpool *pgxpool.Pool, fromBlock uint64, toBlock uint64) {
	for {
		if (toBlock > 0) && (toBlock <= fromBlock) {
			fmt.Printf("Ended on %v\n", toBlock)
			os.Exit(0)
		}
		subFolder := getSubFolder(folder, fromBlock)
		fileName := fmt.Sprintf("%s/%v.json", subFolder, fromBlock)
		content, err := os.ReadFile(fileName)
		if err != nil {
			log.Debug().Msg(fmt.Sprintf("Waiting for new block in %v..", fileName))
			wait()
		} else {
			var block sqlblock.Block
			err := json.Unmarshal([]byte(content), &block)
			if err != nil {
				log.Warn().Msg(fmt.Sprintf("Failed to parse: %+v. Retrying..\n", err))
				wait()
			} else {
				block.Sequence = block.Height
				sql := block.InsertSql()
				// fmt.Println(sql)
				_, err := pgpool.Exec(context.Background(), sql)

				if err != nil {
					log.Warn().Msg(fmt.Sprintf("Unable import block %v: %v\n", block.Height, err))
					wait()
				} else {
					log.Info().Msg(fmt.Sprintf("%+v", block.Height))
					if !keepFiles {
						cleanup(fileName, folder, uint64(block.Height))
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

func cleanup(fileName string, folder string, block uint64) {
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

func getSubFolder(folder string, block uint64) string {
	return fmt.Sprintf("%s/%v", folder, block/10000*10000)
}

func updateRefinerLastBlock(folder string, block uint64) {
	file := fmt.Sprintf("%s/.REFINER_LAST_BLOCK", folder)
	data, err := os.ReadFile(file)
	if err == nil {
		refinerLastBlock, err := strconv.ParseUint(string(data), 10, 64)
		if err == nil && refinerLastBlock < block {
			updateRefinerLastBlockFile(file, block)
		}
	} else {
		updateRefinerLastBlockFile(file, block)
	}
}

func updateRefinerLastBlockFile(file string, block uint64) {
	f, err := os.Create(file)
	if err != nil {
		panic(fmt.Errorf(".REFINER_LAST_BLOCK can not be opened: %v\n", err))
	}
	defer f.Close()
	_, err = f.WriteString(strconv.FormatUint(block, 10))
	if err != nil {
		panic(fmt.Errorf(".REFINER_LAST_BLOCK can not be updated: %v\n", err))
	}
}
