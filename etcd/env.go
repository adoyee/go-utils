package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

var (
	defaultEnv *Env

	errNotExist = errors.New("key not exists")
)

type Env struct {
	cli     *clientv3.Client
	lease   clientv3.Lease
	leaseID clientv3.LeaseID
	sess    *concurrency.Session
}

func InitEnv(ctx context.Context, cli *clientv3.Client, ttl int64) (err error) {
	if defaultEnv != nil {
		log.Fatal("etcd env has been initialized")
	}

	var (
		lease clientv3.Lease
		resp  *clientv3.LeaseGrantResponse
		sess  *concurrency.Session
	)

	lease = clientv3.NewLease(cli)
	resp, err = lease.Grant(ctx, ttl)
	if err != nil {
		_ = lease.Close()
		return
	}

	sess, err = concurrency.NewSession(cli, concurrency.WithLease(resp.ID))
	if err != nil {
		_ = lease.Close()
		return
	}

	ch, err := lease.KeepAlive(ctx, resp.ID)
	if err != nil {
		_ = lease.Close()
		return
	}

	defaultEnv = &Env{
		leaseID: resp.ID,
		lease:   lease,
		sess:    sess,
	}

	go func() {
		for {
			_ = <-ch
		}
	}()

	return
}

func get(ctx context.Context, key string, v interface{}) (err error) {
	resp, err := defaultEnv.cli.Get(ctx, key)
	if err != nil {
		return
	}

	if resp.Count == 0 {
		return errNotExist
	}
	return json.Unmarshal(resp.Kvs[0].Value, v)
}

func put(ctx context.Context, key string, v interface{}) (err error) {
	var buff []byte
	buff, err = json.Marshal(v)
	if err != nil {
		return
	}
	_, err = defaultEnv.cli.Put(ctx, key, string(buff))
	return
}
