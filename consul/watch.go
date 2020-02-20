package consul

import (
	"sync"

	"github.com/abba18/rdiscovery"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"github.com/hashicorp/go-hclog"
)

type Watcher struct {
	sync.Mutex
	watchServices   map[string]*watch.Plan
	watchAllService *watch.Plan
	client          *consulapi.Client
	cache           rdiscovery.Cache
	stop            bool
}

func NewWatcher(client *consulapi.Client, cache rdiscovery.Cache) *Watcher {
	w := &Watcher{
		watchServices: make(map[string]*watch.Plan),
		cache:         cache,
		stop:          false,
		client:        client,
	}

	wp, err := w.WatchAllService()
	if err != nil {
		panic(err)
	}
	w.watchAllService = wp
	go w.watchAllService.RunWithClientAndHclog(client, hclog.New(hclog.DefaultOptions))
	return w
}

func (w *Watcher) Stop() {
	w.stop = true
	w.watchAllService.Stop()
	w.Lock()
	defer w.Unlock()
	for _, wp := range w.watchServices {
		wp.Stop()
	}
}

func (w *Watcher) Watch(serviceName string) {
	w.Lock()
	defer w.Unlock()
	if w.stop {
		return
	}
	if _, ok := w.watchServices[serviceName]; ok {
		// already watch
		return
	}

	wp, err := watch.Parse(map[string]interface{}{
		"type":    "service",
		"service": serviceName,
	})
	if err != nil {
		panic(err)
	}
	wp.Handler = w.watchServiceHandler
	w.watchServices[serviceName] = wp
	go w.watchAllService.RunWithClientAndHclog(w.client, hclog.New(hclog.DefaultOptions))
}

func (w *Watcher) watchServiceHandler(index uint64, res interface{}) {
	services, ok := res.([]*consulapi.ServiceEntry)
	if !ok {
		return
	}
	if len(services) <= 0 {
		return
	}
	serviceName := ""
	nodes := []*rdiscovery.ServiceNode{}
	for _, service := range services {
		serviceName = service.Service.Service
		if service.Checks.AggregatedStatus() == consulapi.HealthPassing {
			node := &rdiscovery.ServiceNode{
				Name:    service.Service.Service,
				ID:      service.Service.ID,
				Address: service.Service.Address,
				Port:    service.Service.Port,
			}
			nodes = append(nodes, node)
		}
	}
	w.cache.Set(serviceName, nodes)
}

func (w *Watcher) WatchAllService() (*watch.Plan, error) {
	wp, err := watch.Parse(map[string]interface{}{
		"type": "services",
	})
	if err != nil {
		return nil, err
	}
	wp.Handler = w.watchAllServiceHandler
	return wp, nil
}

func (w *Watcher) watchAllServiceHandler(index uint64, res interface{}) {
	services, ok := res.(map[string][]string)
	if !ok {
		return
	}
	w.Lock()
	for serviceName := range w.watchServices {
		if _, ok := services[serviceName]; !ok {
			// remove unhealth service
			w.cache.Del(serviceName)
		}
	}
	w.Unlock()
}
