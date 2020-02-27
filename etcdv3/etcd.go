package etcd

import (
	"context"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/abba18/rdiscovery"
	"github.com/coreos/etcd/clientv3"
	jsoniter "github.com/json-iterator/go"
)

type EtcdRdicovery struct {
	prefix  string
	Address []string
	client  *clientv3.Client
	config  *clientv3.Config
	close   chan struct{}
	closed  bool

	// discovery side part
	enableCache bool
	cache       rdiscovery.Cache
	watcher     *Watcher

	// register side part
	register      map[string]*rdiscovery.ServiceNode
	registerLock  sync.Mutex
	keepalivePool map[string]chan struct{}
	keepaliveLock sync.Mutex
}

func NewEtcdRDiscovery(prefix string, address []string, cnf *clientv3.Config, cache rdiscovery.Cache) rdiscovery.Register {
	r := &EtcdRdicovery{
		prefix:        prefix,
		Address:       address,
		config:        cnf,
		close:         make(chan struct{}),
		closed:        false,
		enableCache:   false,
		register:      make(map[string]*rdiscovery.ServiceNode),
		keepalivePool: make(map[string]chan struct{}),
	}
	client, err := r.Client()
	if err != nil {
		panic(err)
	}

	if cache != nil {
		r.cache = cache
		r.enableCache = true
		r.watcher = NewWatcher(client, r.cache)
	}

	return r
}

func (e *EtcdRdicovery) Register(node *rdiscovery.ServiceNode, opt *rdiscovery.Options) error {
	client, err := e.Client()
	if err != nil {
		return err
	}
	e.registerLock.Lock()
	defer e.registerLock.Unlock()
	_, ok := e.register[node.Name]
	if ok {
		return rdiscovery.ErrAlreadyRegitered
	}

	if (opt != nil) && (opt.CheckInterval > 0) {
		ctx := context.TODO()
		ttl := int64(opt.CheckInterval.Seconds())
		grantResp, err := client.Grant(ctx, ttl)
		if err != nil {
			return err
		}
		_, err = client.Put(ctx, newNodeKey(e.prefix, node.Name, node.ID), encode(node), clientv3.WithLease(grantResp.ID))
		if err != nil {
			return nil
		}
		e.keepaliveLock.Lock()
		if c, ok := e.keepalivePool[node.Name]; ok {
			close(c)
		}
		stop := make(chan struct{})
		e.keepalivePool[node.Name] = stop
		e.keepaliveLock.Unlock()
		go e.keepalive(grantResp.ID, stop)
	} else {
		ctx := context.TODO()
		_, err := client.Put(ctx, newNodeKey(e.prefix, node.Name, node.ID), encode(node))
		if err != nil {
			return err
		}
	}
	e.register[node.Name] = node
	return nil
}
func (e *EtcdRdicovery) Deregister(node *rdiscovery.ServiceNode) error {
	e.registerLock.Lock()
	defer e.registerLock.Unlock()
	delete(e.register, node.Name)
	e.keepaliveLock.Lock()
	if c, ok := e.keepalivePool[node.Name]; ok {
		close(c)
		delete(e.keepalivePool, node.Name)
	}
	e.keepaliveLock.Unlock()
	return nil
}
func (e *EtcdRdicovery) GetService(serviceName string) ([]*rdiscovery.ServiceNode, error) {
	client, err := e.Client()
	if err != nil {
		return nil, err
	}
	if e.enableCache {
		nodes, ok := e.cache.Get(serviceName)
		if ok {
			return nodes, nil
		}
	}
	ctx := context.TODO()
	resp, err := client.Get(ctx, newNodeKey(e.prefix, serviceName, ""), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	nodes := []*rdiscovery.ServiceNode{}
	for _, v := range resp.Kvs {
		node := decode(string(v.Value))
		nodes = append(nodes, node)
	}

	if e.enableCache {
		go e.watcher.Watch(serviceName)
	}

	return nodes, nil
}
func (e *EtcdRdicovery) Close() {
	e.client.Close()
	e.closed = false
	close(e.close)

	del := map[string]*rdiscovery.ServiceNode{}
	e.registerLock.Lock()
	for k, v := range e.register {
		del[k] = v
	}
	e.registerLock.Unlock()
	for _, v := range del {
		e.Deregister(v)
	}
}

func (e *EtcdRdicovery) Client() (*clientv3.Client, error) {
	if e.client != nil {
		return e.client, nil
	}

	if len(e.Address) <= 0 {
		return nil, nil
	}

	if e.config == nil {
		e.config = &clientv3.Config{
			// Endpoints: e.Address,
		}
	}
	e.config.Endpoints = e.Address
	client, err := clientv3.New(*e.config)
	if err != nil {
		return nil, err
	}
	e.client = client
	return e.client, nil
}

func (e *EtcdRdicovery) keepalive(leaseID clientv3.LeaseID, stop chan struct{}) error {
	client, err := e.Client()
	if err != nil {
		return nil
	}
	for {
		if e.closed {
			return nil
		}
		ctx := context.TODO()
		resp, err := client.KeepAlive(ctx, leaseID)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		for {
			select {
			case _, ok := <-resp:
				if !ok {
					break
				}
			case <-e.close:
				ctx := context.TODO()
				client.Revoke(ctx, leaseID)
				return nil
			case <-stop:
				ctx := context.TODO()
				client.Revoke(ctx, leaseID)
				return nil
			}
		}
	}
}

func newNodeKey(prefix, name, id string) string {
	name = strings.Replace(name, "/", "-", -1)
	id = strings.Replace(id, "/", "-", -1)
	return path.Join(prefix, name, id)
}

func encode(node *rdiscovery.ServiceNode) string {
	data, _ := jsoniter.MarshalToString(node)
	return data
}

func decode(value string) *rdiscovery.ServiceNode {
	node := &rdiscovery.ServiceNode{}
	jsoniter.UnmarshalFromString(value, node)
	return node
}
