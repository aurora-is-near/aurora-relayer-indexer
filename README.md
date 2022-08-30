# Aurora Relayer Indexer

Aurora Relayer Indexer used to continuously populate postgres database with blocks. It relies
on [borealis-engine-lib](https://github.com/aurora-is-near/borealis-engine-lib) to generate data.
Schema for that database
located in the [aurora-relayer](https://github.com/aurora-is-near/aurora-relayer) repo.

## How to build
```bash
go build #mac
make #linux
```

## Template for config.yaml:
```yaml
---
database: postgres://aurora:aurora@database/aurora
debug: false
keepFiles: false
sourceFolder: ../borealis-engine-lib/output/refiner
genesisBlock: 9820210
fromBlock: 0
toBlock: 0
```

## How to use

```bash
Usage:
  indexer [flags]

Flags:
  -c, --config string         config file (default is config/local.yaml)
      --database string       database url (default is postgres://aurora:aurora@database/aurora)
  -d, --debug                 enable debugging(default is false)
  -f, --fromBlock uint        block to start from. Ignored if missing or 0 (default is 0)
  -g, --genesisBlock uint     aurora genesis block. It will never index blocks prior to it. (default is 1)
  -h, --help                  help for indexer
  -k, --keepFiles             keep json files(default is false)
  -s, --sourceFolder string   source folder populated with block.json files. (default "../borealis-engine-lib/output/refiner")
  -t, --toBlock uint          block to end on. Ignored if missing or 0 (default is 0)
  -v, --version               version for indexer
```

## Example of usage

```bash
./indexer # Using config from `config/local.yaml`
./indexer --config config/mainnet.yaml # Using different config file
./indexer --fromBlock 30000000 # Flags will override config set in yaml file
```
