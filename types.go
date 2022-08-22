package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/doug-martin/goqu/v9/exp"

	"github.com/btcsuite/btcutil/base58"
)

type Uint256 string
type Uint64 uint64
type H256 string
type Address string

func (val Uint256) toSqlValue() string {
	bigInt, ok := new(big.Int).SetString(string(val), 0)
	if ok {
		return bigInt.Text(10)
	} else {
		return "0"
	}
}

func (val Uint64) toSqlValue() string {
	return strconv.FormatUint(uint64(val), 10)
}

type insertData map[string]interface{}

type Block struct {
	ChainId          Uint64        `cbor:"chain_id" json:"chain_id"`
	Hash             H256          `cbor:"hash" json:"hash"`
	ParentHash       H256          `cbor:"parent_hash" json:"parent_hash"`
	Height           Uint64        `cbor:"height" json:"height"`
	Miner            Address       `cbor:"miner" json:"miner"`
	Timestamp        int64         `cbor:"timestamp" json:"timestamp"`
	GasLimit         Uint256       `cbor:"gas_limit" json:"gas_limit"`
	GasUsed          Uint256       `cbor:"gas_used" json:"gas_used"`
	LogsBloom        string        `cbor:"logs_bloom" json:"logs_bloom"`
	TransactionsRoot H256          `cbor:"transactions_root" json:"transactions_root"`
	ReceiptsRoot     H256          `cbor:"receipts_root" json:"receipts_root"`
	Transactions     []Transaction `cbor:"transactions" json:"transactions"`
	NearBlock        any           `cbor:"near_metadata" json:"near_metadata"`
	StateRoot        string        `cbor:"state_root" json:"state_root"`
	Size             Uint256       `cbor:"size" json:"size"`
	Sequence         Uint64
}

type Transaction struct {
	Hash                 H256            `cbor:"hash" json:"hash"`
	BlockHash            H256            `cbor:"block_hash" json:"block_hash"`
	BlockHeight          Uint64          `cbor:"block_height" json:"block_height"`
	ChainId              Uint64          `cbor:"chain_id" json:"chain_id"`
	TransactionIndex     uint32          `cbor:"transaction_index" json:"transaction_index"`
	From                 Address         `cbor:"from" json:"from"`
	To                   Address         `cbor:"to" json:"to"`
	Nonce                Uint256         `cbor:"nonce" json:"nonce"`
	GasPrice             Uint256         `cbor:"gas_price" json:"gas_price"`
	GasLimit             Uint256         `cbor:"gas_limit" json:"gas_limit"`
	GasUsed              Uint64          `cbor:"gas_used" json:"gas_used"`
	MaxPriorityFeePerGas Uint256         `cbor:"max_priority_fee_per_gas" json:"max_priority_fee_per_gas"`
	MaxFeePerGas         Uint256         `cbor:"max_fee_per_gas" json:"max_fee_per_gas"`
	Value                Uint256         `cbor:"value" json:"value"`
	Input                []byte          `cbor:"input" json:"input"`
	Output               []byte          `cbor:"output" json:"output"`
	AccessList           []AccessList    `cbor:"access_list" json:"access_list"`
	TxType               uint8           `cbor:"tx_type" json:"tx_type"`
	Status               bool            `cbor:"status" json:"status"`
	Logs                 []Log           `cbor:"logs" json:"logs"`
	LogsBloom            string          `cbor:"logs_bloom" json:"logs_bloom"`
	ContractAddress      Address         `cbor:"contract_address" json:"contract_address"`
	V                    Uint64          `cbor:"v" json:"v"`
	R                    Uint256         `cbor:"r" json:"r"`
	S                    Uint256         `cbor:"s" json:"s"`
	NearTransaction      NearTransaction `cbor:"near_metadata" json:"near_metadata"`
}

type AccessList struct {
	Address     Address `cbor:"address" json:"address"`
	StorageKeys []H256  `cbor:"storageKeys" json:"storageKeys"`
}

type Log struct {
	Address Address  `cbor:"Address" json:"Address"`
	Topics  [][]byte `cbor:"Topics" json:"Topics"`
	Data    []byte   `cbor:"data" json:"data"`
}

type ExistingBlock struct {
	NearHash       string `cbor:"near_hash" json:"near_hash"`
	NearParentHash string `cbor:"near_parent_hash" json:"near_parent_hash"`
	Author         string `cbor:"author" json:"author"`
}

type NearTransaction struct {
	Hash        string `cbor:"hash" json:"hash"`
	ReceiptHash string `cbor:"receipt_hash" json:"receipt_hash"`
}

func (block Block) insertData() insertData {
	var existingBlock ExistingBlock

	switch nearBlock := block.NearBlock.(type) {
	case map[interface{}]interface{}:
		parsedExistingBlock := nearBlock["ExistingBlock"].(map[interface{}]interface{})
		existingBlock = ExistingBlock{
			NearHash:       parsedExistingBlock["near_hash"].(string),
			NearParentHash: parsedExistingBlock["near_parent_hash"].(string),
			Author:         parsedExistingBlock["author"].(string),
		}
	case string:
		existingBlock = ExistingBlock{}
	}

	return insertData{
		"chain":             block.ChainId,
		"id":                block.Height,
		"hash":              withHexPrefix(block.Hash),
		"near_hash":         withHexPrefix(hex.EncodeToString(base58.Decode(existingBlock.NearHash))),
		"timestamp":         time.Unix(block.Timestamp/1000000000, 0),
		"size":              block.Size.toSqlValue(),
		"gas_limit":         block.GasLimit.toSqlValue(),
		"gas_used":          block.GasUsed.toSqlValue(),
		"parent_hash":       withHexPrefix(block.ParentHash),
		"transactions_root": withHexPrefix(block.TransactionsRoot),
		"state_root":        withHexPrefix(block.StateRoot),
		"receipts_root":     withHexPrefix(block.ReceiptsRoot),
		"logs_bloom":        withHexPrefix(block.LogsBloom[2:]),
		"miner":             withHexPrefix(block.Miner),
		"author":            existingBlock.Author,
		"sequence":          block.Sequence,
	}
}

func (transaction Transaction) insertData() insertData {
	var input *string
	if len(transaction.Input) > 0 {
		i := hex.EncodeToString(transaction.Input)
		input = withHexPrefix(i)
	}
	var output *string
	if len(transaction.Output) > 0 {
		o := hex.EncodeToString(transaction.Output)
		output = withHexPrefix(o)
	}

	accessList, _ := json.Marshal(transaction.AccessList)
	receiptHash := hex.EncodeToString(base58.Decode(transaction.NearTransaction.ReceiptHash))
	return insertData{
		"block":                    transaction.BlockHeight,
		"block_hash":               withHexPrefix(transaction.BlockHash),
		"index":                    transaction.TransactionIndex,
		"hash":                     withHexPrefix(transaction.Hash),
		"near_hash":                withHexPrefix(receiptHash),
		"near_receipt_hash":        withHexPrefix(receiptHash),
		"from":                     withHexPrefix(transaction.From),
		"to":                       withHexPrefix(transaction.To),
		"nonce":                    transaction.Nonce.toSqlValue(),
		"gas_price":                transaction.GasPrice.toSqlValue(),
		"gas_limit":                transaction.GasLimit.toSqlValue(),
		"gas_used":                 transaction.GasUsed.toSqlValue(),
		"value":                    transaction.Value.toSqlValue(),
		"input":                    input,
		"v":                        transaction.V.toSqlValue(),
		"r":                        transaction.R.toSqlValue(),
		"s":                        transaction.S.toSqlValue(),
		"status":                   transaction.Status,
		"logs_bloom":               withHexPrefix(transaction.LogsBloom[2:]),
		"output":                   output,
		"access_list":              accessList,
		"max_fee_per_gas":          transaction.MaxFeePerGas.toSqlValue(),
		"max_priority_fee_per_gas": transaction.MaxPriorityFeePerGas.toSqlValue(),
		"type":                     transaction.TxType,
		"contract_address":         withHexPrefix(transaction.ContractAddress),
	}
}

func (block Block) insertSql() string {
	dialect := goqu.Dialect("postgres")
	data := block.insertData()
	ds := dialect.Insert("block").Rows(data)
	sql, _, _ := ds.ToSQL()
	sql = fmt.Sprintf("WITH b AS (%s ON CONFLICT DO NOTHING)", sql)
	transactionSql := ""
	var eventDataset *goqu.SelectDataset

	for transactionIndex, transaction := range block.Transactions {
		data = transaction.insertData()
		updates := make([]string, 0, len(data))

		for k := range data {
			updates = append(updates, fmt.Sprintf("\"%v\" = EXCLUDED.%v", k, k))
		}

		ds := dialect.Insert(exp.ParseIdentifier("transaction").As("t")).Rows(data)
		s, _, _ := ds.ToSQL()
		s = fmt.Sprintf(", tx%v AS (%s ON CONFLICT (hash) DO UPDATE SET %v WHERE t.status = false AND EXCLUDED.status = true RETURNING id)", transactionIndex, s, strings.Join(updates[:], ", "))
		transactionSql = transactionSql + s

		for eventIndex, log := range transaction.Logs {
			var data string
			if len(log.Data) > 0 {
				data = hex.EncodeToString(log.Data)
			}
			var topics []string
			for _, topic := range log.Topics {
				topics = append(topics, fmt.Sprintf("'%s'", *withHexPrefix(hex.EncodeToString(topic))))
			}

			selectColumns := make([]interface{}, 0, 9)
			selectColumns = append(selectColumns, exp.ParseIdentifier(fmt.Sprintf("tx%v.id", transactionIndex)))
			selectColumns = append(selectColumns, goqu.V(eventIndex))
			selectColumns = append(selectColumns, goqu.Cast(goqu.V(withHexPrefix(data)), "bytea"))
			selectColumns = append(selectColumns, goqu.Cast(goqu.V(withHexPrefix(log.Address)), "address"))
			selectColumns = append(selectColumns, goqu.L(fmt.Sprintf("ARRAY[%s]::hash[]", strings.Join(topics, ", "))))
			selectColumns = append(selectColumns, goqu.V(block.Height))
			selectColumns = append(selectColumns, goqu.Cast(goqu.V(withHexPrefix(block.Hash)), "hash"))
			selectColumns = append(selectColumns, goqu.V(transaction.TransactionIndex))
			selectColumns = append(selectColumns, goqu.Cast(goqu.V(withHexPrefix(transaction.Hash)), "hash"))

			sel := dialect.Select(selectColumns...).From(exp.ParseIdentifier(fmt.Sprintf("tx%v", transactionIndex)))
			if eventDataset == nil {
				eventDataset = sel
			} else {
				eventDataset = eventDataset.Union(sel)
			}
		}
	}

	var eventSql string
	if eventDataset == nil {
		es := dialect.Select(goqu.V(1))
		eventSql, _, _ = es.ToSQL()
	} else {
		es := dialect.Insert("event").Cols("transaction", "index", "data", "from", "topics", "block", "block_hash", "transaction_index", "transaction_hash").FromQuery(eventDataset)
		eventSql, _, _ = es.ToSQL()
	}
	return sql + transactionSql + eventSql
}

func withHexPrefix[T string | H256 | Address | byte | []byte](hash T) *string {
	str := string(hash)
	if len(str) > 0 {
		str = strings.Replace(str, "0x", "\\x", 1)
		if !strings.HasPrefix(str, "\\x") {
			str = "\\x" + str
		}
		return &str
	} else {
		return nil
	}
}
