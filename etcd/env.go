// a simple wrapper of etcd
package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	nm "github.com/coreos/etcd/clientv3/namespace"
)

var (
	defaultEnv  *Env
	KeyNotExist = errors.New("key not exists")
)

type Env struct {
	cli     *clientv3.Client
	lease   clientv3.Lease
	leaseID clientv3.LeaseID
	sess    *concurrency.Session
}

func newEnv(ctx context.Context, ep []string, namespace string, ttl int64) (env *Env, err error) {
	var (
		config clientv3.Config
		cli    *clientv3.Client
		lease  clientv3.Lease
		resp   *clientv3.LeaseGrantResponse
		sess   *concurrency.Session
	)

	config.Endpoints = ep
	config.Context = ctx

	if cli, err = clientv3.New(config); err != nil {
		return
	}

	cli.KV = nm.NewKV(cli.KV, namespace)
	cli.Lease = nm.NewLease(cli.Lease, namespace)
	cli.Watcher = nm.NewWatcher(cli.Watcher, namespace)

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

	env = &Env{
		cli:     cli,
		lease:   lease,
		leaseID: resp.ID,
		sess:    sess,
	}

	go func() {
		for {
			_ = <-ch
		}
	}()

	return
}

func InitEnv(ctx context.Context, ep []string, namespace string, ttl int64) (err error) {
	if defaultEnv != nil {
		log.Fatal("etcd env has been initialized")
	}

	var env *Env

	if env, err = newEnv(ctx, ep, namespace, ttl); err != nil {
		return
	}
	defaultEnv = env
	return
}

func (env *Env) Get(ctx context.Context, prefix string, v interface{}) (err error) {
	var resp *clientv3.GetResponse
	if resp, err = env.cli.Get(ctx, prefix); err != nil {
		return
	}
	if resp.Count == 0 {
		return KeyNotExist
	}

	return json.Unmarshal(resp.Kvs[0].Value, v)
}

func (env *Env) GetWithPrefix(ctx context.Context, prefix string) (resp *clientv3.GetResponse, err error) {
	resp, err = env.cli.Get(ctx, prefix, clientv3.WithPrefix())
	return
}

func (env *Env) Put(ctx context.Context, prefix string, v interface{}) (err error) {
	var buff []byte
	buff, err = json.Marshal(v)
	if err != nil {
		return
	}
	_, err = env.cli.Put(ctx, prefix, string(buff))
	return
}

func (env *Env) PutWithLease(ctx context.Context, prefix string, v interface{}) (err error) {
	var buff []byte
	buff, err = json.Marshal(v)
	if err != nil {
		return
	}
	_, err = env.cli.Put(ctx, prefix, string(buff), clientv3.WithLease(env.leaseID))
	return
}

func (env *Env) Delete(ctx context.Context, prefix string, opts ...clientv3.OpOption) (err error) {
	_, err = env.cli.Delete(ctx, prefix, opts...)
	return
}

func (env *Env) Exist(ctx context.Context, prefix string) (exist bool, err error) {
	var resp *clientv3.GetResponse
	if resp, err = env.cli.Get(ctx, prefix); err != nil {
		return
	}
	exist = resp.Count > 0
	return
}

func (env *Env) Lock(ctx context.Context, prefix, check string) (key string, ok bool) {
	var err error
	mutex := concurrency.NewMutex(env.sess, prefix)
	if err = mutex.Lock(ctx); err != nil {
		return
	}

	if check == "" {
		return mutex.Key(), true
	}

	exist, err := env.Exist(ctx, check)
	if err != nil {
		_ = mutex.Unlock(ctx)
		return "", false
	}

	if !exist {
		return "", false
	}

	return mutex.Key(), true
}

func (env *Env) Unlock(ctx context.Context, key string) (err error) {
	_, err = env.cli.Delete(ctx, key)
	return
}

func (env *Env) CountPrefix(ctx context.Context, prefix string, opts ...clientv3.OpOption) (n int64, err error) {
	var resp *clientv3.GetResponse

	op := []clientv3.OpOption{clientv3.WithCountOnly()}
	op = append(op, opts...)
	resp, err = env.cli.Get(ctx, prefix, op...)
	if err != nil {
		return
	}
	n = resp.Count
	return
}

func (env *Env) Watch(ctx context.Context, prefix string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return env.cli.Watch(ctx, prefix, opts...)
}

func Get(ctx context.Context, prefix string, value interface{}) error {
	return defaultEnv.Get(ctx, prefix, value)
}

func GetWithPrefix(ctx context.Context, prefix string) (*clientv3.GetResponse, error) {
	return defaultEnv.GetWithPrefix(ctx, prefix)
}

func Put(ctx context.Context, prefix string, value interface{}) error {
	return defaultEnv.Put(ctx, prefix, value)
}

func PutWithLease(ctx context.Context, prefix string, value interface{}) error {
	return defaultEnv.PutWithLease(ctx, prefix, value)
}

func Exist(ctx context.Context, prefix string) (bool, error) {
	return defaultEnv.Exist(ctx, prefix)
}

func Lock(ctx context.Context, prefix, check string) (string, bool) {
	return defaultEnv.Lock(ctx, prefix, check)
}

func Unlock(ctx context.Context, key string) error {
	return defaultEnv.Unlock(ctx, key)
}

func Delete(ctx context.Context, prefix string, opts ...clientv3.OpOption) error {
	return defaultEnv.Delete(ctx, prefix, opts...)
}
func CountPrefix(ctx context.Context, prefix string, opts ...clientv3.OpOption) (int64, error) {
	return defaultEnv.CountPrefix(ctx, prefix, opts...)
}

func Watch(ctx context.Context, prefix string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return defaultEnv.Watch(ctx, prefix, opts...)
}
