package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/sirupsen/logrus"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db   *badger.DB
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{conf: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = s.conf.DBPath
	opts.ValueDir = s.conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		logrus.Errorf("badger open error, err = %v", err)
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	//todo 这里的事务是怎噩梦处理的，自己每次新建一个?
	txn := s.db.NewTransaction(true)
	return standAloneReader{inner: s, txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	//todo 这里的事务是怎么处理的
	for _, modify := range batch {
		var err error
		if put, ok := modify.Data.(storage.Put); ok {
			err = engine_util.PutCF(s.db, put.Cf, put.Key, put.Value)
			if err != nil {
				logrus.Errorf("PutCF error, err = %v", err)
				return err
			}
		}
		if del, ok := modify.Data.(storage.Delete); ok {
			err = engine_util.DeleteCF(s.db, del.Cf, del.Key)
			if err != nil {
				logrus.Errorf("DeleteCF error, err = %v", err)
				return err
			}
		}
	}
	return nil
}

type standAloneReader struct {
	txn   *badger.Txn
	inner *StandAloneStorage
}

func (r standAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r standAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r standAloneReader) Close() {
	r.txn.Discard()
}
