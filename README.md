⚠ WARNING: This repository is no longer maintained. New indexer is integrated into [Aurora Relayer V2](https://github.com/aurora-is-near/relayer2-public).

# Aurora Relayer Indexer

Aurora Relayer Indexer used to continuously populate postgres database with blocks. It relies
on [borealis-engine-lib](https://github.com/aurora-is-near/borealis-engine-lib) to generate data.
Schema for that database
located in the [aurora-relayer](https://github.com/aurora-is-near/aurora-relayer/blob/indexer/docker/docker-entrypoint-initdb.d/init.txt) repo.

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

## Options

### Config

Provide config file with `--config` flag. If not provided, it will try to load `config/local.yaml`.

#### Parameters
```bash
./indexer --config config/mainnet.yaml
```

### Database

Required parameter. Provide database url with `--database` flag. If not provided, it will try to load `database` from the local config file: `config/local.yaml`. Config file will override parameter.

#### Parameters
```bash
./indexer --database postgres://aurora:aurora@database/aurora
```

#### Config options

```yaml
database: postgres://aurora:aurora@database/aurora
```

### Debug

Default value: `false`

Enable debug mode with `--debug` flag. If not provided, it will try to load `debug` from the local config file: `config/local.yaml`. Config file will override parameter.

#### Parameters
```bash
./indexer --debug
```

#### Config options

```yaml
debug: true
```

### keepFiles

Default value: `false`

Keep json files after processing with `--keepFiles` flag. If not provided, it will try to load `keepFiles` from the local config file: `config/local.yaml`. Config file will override parameter.

#### Parameters
```bash
./indexer --keepFiles
```

### sourceFolder

Default value: `../borealis-engine-lib/output/refiner`

Provide source folder with `--sourceFolder` flag. If not provided, it will try to load `sourceFolder` from the local config file: `config/local.yaml`. Config file will override parameter.

#### Parameters
```bash
./indexer --sourceFolder ../borealis-engine-lib/output/refiner
```

#### Config options

```yaml
sourceFolder: ../borealis-engine-lib/output/refiner
```

### fromBlock

Default value: `0`

Provide block to start from with `--fromBlock` flag. If not provided, it will try to load `fromBlock` from the local config file: `config/local.yaml`. Config file will override parameter.

#### Parameters
```bash
./indexer --fromBlock 0
```

#### Config options

```yaml
fromBlock: 0
```

### toBlock

Provide block to end on with `--toBlock` flag. If not provided, it will try to load `toBlock` from the local config file: `config/local.yaml`. Config file will override parameter.

Default value: `0`

#### Parameters
```bash
./indexer --toBlock 0
```

#### Config options

```yaml
toBlock: 0
```

### genesisBlock

Provide aurora genesis block with `--genesisBlock` flag. If not provided, it will try to load `genesisBlock` from the local config file: `config/local.yaml`. Config file will override parameter.

Default value: `1`

#### Parameters
```bash
./indexer --genesisBlock 9820210
```

#### Config options

```yaml
genesisBlock: 9820210
```

## Example of usage

```bash
./indexer # Using config from `config/local.yaml`
./indexer --config config/mainnet.yaml # Using different config file
./indexer --fromBlock 30000000 # Flags will override config set in yaml file
```

## How to test
1. `cp config/test.yaml_example config/test.yaml`
2. Modify `database` in `config/test.yaml` file.
3. Create postgres database from this [schema](https://github.com/aurora-is-near/aurora-relayer/blob/indexer/docker/docker-entrypoint-initdb.d/init.txt)
```
psql -d postgres
CREATE DATABASE aurora_relayer_indexer;
exit

cd aurora-is-near/aurora-relayer/docker/docker-entrypoint-initdb.d (indexer branch)
psql -v ON_ERROR_STOP=1 aurora_relayer_indexer < init.txt
```
4. Run `go test`
