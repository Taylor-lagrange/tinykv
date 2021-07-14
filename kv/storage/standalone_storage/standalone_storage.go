package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type StandAloneStorageReader struct {
	txn *badger.Txn
}

// GetCF can't return err because err not found means value == nil
func (s StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	v, _ := engine_util.GetCFFromTxn(s.txn, cf, key)
	return v, nil
}

func (s StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s StandAloneStorageReader) Close() {
	s.txn.Discard()
}

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kv := engine_util.CreateDB(conf.DBPath, false)
	return &StandAloneStorage{
		engine: engine_util.NewEngines(kv, nil, conf.DBPath, ""),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	//s.engine.Kv
	return StandAloneStorageReader{txn: s.engine.Kv.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := engine_util.WriteBatch{}
	for _, op := range batch {
		switch op.Data.(type) {
		case storage.Put:
			wb.SetCF(op.Cf(), op.Key(), op.Value())
		case storage.Delete:
			wb.DeleteCF(op.Cf(), op.Key())
		}
	}
	if err := wb.WriteToDB(s.engine.Kv); err != nil {
		return err
	}
	return nil
}
