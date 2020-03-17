package etcd

import (
	"context"
	"sync"

	"github.com/abba18/rdiscovery"
	"github.com/coreos/etcd/clientv3"
)

type Watcher struct {
	sync.Mutex
	watchService map[string]struct{}
	cache        rdiscovery.Cache
	client       *clientv3.Client
	events       chan []*clientv3.Event
	stop         bool
	close        chan struct{}
}

func NewWatcher(client *clientv3.Client, cache rdiscovery.Cache) *Watcher {
	w := &Watcher{
		watchService: make(map[string]struct{}),
		stop:         false,
		events:       make(chan []*clientv3.Event, 100),
		close:        make(chan struct{}),
		client:       client,
	}
	go w.handler()
	return w
}

func (w *Watcher) Watch(serviceName string) {
	w.Lock()
	_, ok := w.watchService[serviceName]
	if ok {
		return
	}
	w.watchService[serviceName] = struct{}{}
	w.Unlock()
	for {
		ctx := context.TODO()
		ch := w.client.Watch(ctx, serviceName)
		for {
			select {
			case resp, ok := <-ch:
				if !ok {
					resp.Err()
					break
				}
				w.events <- resp.Events
			case <-w.close:
				close(w.events)
				return
			}
		}
	}

}

func (w *Watcher) handler() {
	for {
		select {
		case events, ok := <-w.events:
			if !ok {
				return
			}
			for _, event := range events {
				switch event.Type {
				case clientv3.EventTypeDelete:
					node := decode(string(event.Kv.Value))
					newNodes := []*rdiscovery.ServiceNode{}
					if nodes, ok := w.cache.Get(node.Name); ok {
						for i := range nodes {
							if nodes[i].ID == node.ID {
								newNodes = append(newNodes, nodes[i+1:len(nodes)]...)
								break
							}
							newNodes = append(newNodes, nodes[i])
						}
					}
					w.cache.Set(node.Name, newNodes)
				case clientv3.EventTypePut:
					node := decode(string(event.Kv.Value))
					if nodes, ok := w.cache.Get(node.Name); ok {
						for i := range nodes {
							if nodes[i].ID == node.ID {
								nodes[i] = node
								w.cache.Set(node.Name, nodes)
								break
							}
						}
					}
				}
			}
		}
	}
}

func (w *Watcher) Close() {
	w.client.Watcher.Close()
	close(w.close)
}
