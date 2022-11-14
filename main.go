package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
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

const pgPoolExecTimeout = 10000 * time.Millisecond

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

// initConfig reads in config file if set.
func initConfig() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	if configFile != "" {
		log.Warn().Msgf("Using config file: %v", viper.ConfigFileUsed())
		viper.SetConfigFile(configFile)
	} else {
		viper.AddConfigPath("config")
		viper.AddConfigPath("../../config")
		viper.SetConfigName("local")
		if err := viper.BindPFlags(rootCmd.PersistentFlags()); err != nil {
			log.Panic().Err(err).Msg("Flags are not bindable")
		}
	}
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err == nil {
		log.Warn().Msgf("Using config file: %v", viper.ConfigFileUsed())
	}
	debug = viper.GetBool("debug")
	databaseURL = viper.GetString("database")
	sourceFolder = viper.GetString("sourceFolder")
	fromBlock = viper.GetUint64("fromBlock")
	toBlock = viper.GetUint64("toBlock")
	genesisBlock = viper.GetUint64("genesisBlock")

	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:     "indexer",
	Short:   "Consumes json files to produce blocks.",
	Long:    `Consumes json files to produce blocks. Generates sql and inserts them to db.`,
	Version: "0.0.1",
	Run: func(cmd *cobra.Command, args []string) {
		pgpool, err := pgxpool.Connect(context.Background(), databaseURL)
		if err != nil {
			log.Panic().Err(err).Msgf("Unable to connect to database: %s", databaseURL)
		}
		defer pgpool.Close()

		if fromBlock == 0 {
			fromBlock, err = getPendingBlockHeight(pgpool)
			if err != nil {
				log.Panic().Err(err).Msgf("Can not retrieve last indexed block from db: %s", databaseURL)
			}
		}

		if fromBlock < genesisBlock {
			fromBlock = genesisBlock
		}

		interrupt := make(chan os.Signal, 10)
		signal.Notify(interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
		indexBlocks(interrupt, sourceFolder, pgpool, fromBlock, toBlock)
	},
}

// indexBlocks indexes blocks from sourceFolder to databaseURL
func indexBlocks(interrupt chan os.Signal, folder string, pgpool *pgxpool.Pool, fromBlock, toBlock uint64) {
	for !(toBlock > 0 && toBlock <= fromBlock) {
		select {
		case <-interrupt:
			return
		default:
		}

		subFolder := getSubFolder(folder, fromBlock)
		fileName := fmt.Sprintf("%s/%v.json", subFolder, fromBlock)
		content, err := os.ReadFile(fileName)
		if err != nil {
			log.Debug().Msgf("Waiting for new block in %v..", fileName)
			wait()
		} else {
			var block sqlblock.Block
			err := json.Unmarshal(content, &block)
			if err != nil {
				log.Warn().Msgf("Failed to parse: %+v. Retrying..\n", err)
				wait()
			} else {
				block.Sequence = block.Height
				sql := block.InsertSql()
				// fmt.Println(sql)
				ctx, cancel := context.WithTimeout(context.Background(), pgPoolExecTimeout)
				_, err := pgpool.Exec(ctx, sql)
				cancel()

				if err != nil {
					log.Warn().Msgf("Unable import block %v: %v\n", block.Height, err)
					wait()
				} else {
					log.Info().Msgf("%+v", block.Height)
					if !keepFiles {
						cleanup(fileName, folder, uint64(block.Height))
					}
					fromBlock++
				}
			}
		}
	}

	fmt.Printf("Ended on %v\n", toBlock)
}

// getPendingBlockHeight returns the last block height from the database
func getPendingBlockHeight(pgpool *pgxpool.Pool) (uint64, error) {
	var blockID uint64
	err := pgpool.QueryRow(context.Background(),
		"SELECT COALESCE(MAX(id), 0) FROM block").Scan(&blockID)
	if err != nil {
		return 0, err
	}
	return blockID + 1, nil
}

// cleanup removes the file and the folder
func cleanup(fileName, folder string, block uint64) {
	err := os.Remove(fileName)
	if err != nil {
		log.Warn().Msgf("Unable to remove file %v: %v\n", fileName, err)
	}
	if block == block/10000*10000 {
		subFolder := getSubFolder(folder, block-1)
		err := os.Remove(subFolder)
		if err != nil {
			log.Warn().Msgf("Unable to remove folder %v: %v\n", subFolder, err)
		}
	}
}

func wait() {
	time.Sleep(500 * time.Millisecond)
}

// getSubFolder returns the subfolder for the block
func getSubFolder(folder string, block uint64) string {
	return fmt.Sprintf("%s/%v", folder, block/10000*10000)
}
