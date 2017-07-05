package memo

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"golang.org/x/net/context"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	kvs "github.com/docker/libkv/store/memo/memo_kvs"
)

type Memo struct {
  kvs kvs.KeyValueStoreClient
}

func Register() {
  libkv.AddStore("memo", New)
}

func New(addrs []string, options *store.Config) (store.Store, error) {
	conn, err := grpc.Dial(addrs[0], grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	kvs := kvs.NewKeyValueStoreClient(conn)
	return &Memo{kvs: kvs}, nil
}

func (s *Memo) Get(key string) (*store.KVPair, error) {
	res, err := s.kvs.Fetch(context.Background(), &kvs.FetchRequest{Key: key})
	if err != nil {
		if grpc.Code(err) == codes.NotFound {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}
	return &store.KVPair {
		Key: key,
		Value: res.Value,
		LastIndex: 0,
	}, nil
}

func (s *Memo) Put(key string, value []byte, options *store.WriteOptions) error {
	_, err := s.kvs.Upsert(context.Background(),
		&kvs.UpsertRequest{Key: key, Value: value})
	return err
}

func (s *Memo) Delete(key string) error {
	_, err := s.kvs.Delete(context.Background(), &kvs.DeleteRequest{Key: key})
	return err
}

func (s *Memo) Exists(key string) (bool, error) {
	_, err := s.kvs.Fetch(context.Background(), &kvs.FetchRequest{Key: key})
	if err == nil {
		return true, nil
	}
	if grpc.Code(err) == codes.NotFound {
		return false, nil
	}
	return false, err
}

func (s *Memo) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (s *Memo) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (s *Memo) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, store.ErrCallNotSupported
}

func (s *Memo) List(directory string) ([]*store.KVPair, error) {
	keys, err := s.kvs.List(context.Background(),
		&kvs.ListRequest{Prefix: directory, MaxKeys: 1000000000})
	if err != nil {
		return nil, err
	}
	var res []*store.KVPair
	for _,k := range(keys.Items) {
		kv, err := s.Get(k.Key)
		if err != nil {
			return nil, err
		}
		res = append(res, kv)
	}
	return res, nil
}

func (s *Memo) DeleteTree(directory string) error {
	keys, err := s.kvs.List(context.Background(),
		&kvs.ListRequest{Prefix: directory, MaxKeys: 1000000000})
	if err != nil {
		return err
	}
	for _, k := range(keys.Items) {
		s.Delete(k.Key)
	}
	return nil
}

func (s *Memo) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	return false, nil, store.ErrCallNotSupported
}

func (s *Memo) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	return false, store.ErrCallNotSupported
}

func (s *Memo) Close() {
}