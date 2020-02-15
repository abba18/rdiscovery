package consul

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/abba18/rdiscovery"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
)

type ConsulRdicovery struct {
	// common part
	Address []string
	client  *consulapi.Client
	config  *consulapi.Config

	// discovery side part
	cache        rdiscovery.Cache
	watchService map[string]struct{}
	watchLock    sync.Mutex
}

func NewConsulRdiscovery(address []string, cfg *consulapi.Config, c rdiscovery.Cache) (rdiscovery.Register, error) {
	reg := &ConsulRdicovery{
		Address:      address,
		config:       cfg,
		watchService: make(map[string]struct{}),
	}
	if c == nil {
		reg.cache = rdiscovery.NewRCache()
	}
	if _, err := reg.Client(); err != nil {
		return nil, err
	}
	go func() {
		if err := reg.WatchAllService(); err != nil {
			panic(err)
		}
	}()
	return reg, nil
}

func (c *ConsulRdicovery) Register(Node *rdiscovery.ServiceNode, opt *rdiscovery.Options) error {
	client, err := c.Client()
	if err != nil {
		return err
	}

	check := getAgentServiceCheckByOpt(opt)

	asr := &consulapi.AgentServiceRegistration{
		ID:      Node.ID,
		Name:    Node.Name,
		Address: Node.Address,
		Port:    Node.Port,
		Check:   check,
	}
	if err := client.Agent().ServiceRegister(asr); err != nil {
		return err
	}
	return nil
}
func (c *ConsulRdicovery) Deregister(Node *rdiscovery.ServiceNode) error {
	client, err := c.Client()
	if err != nil {
		return err
	}
	client.Agent().ServiceDeregister(Node.ID)
	return nil
}

func (c *ConsulRdicovery) GetService(serviceName string) ([]*rdiscovery.ServiceNode, error) {
	client, err := c.Client()
	if err != nil {
		return nil, err
	}
	// cache first
	if nodes, ok := c.cache.Get(serviceName); ok {
		return nodes, nil
	}

	services, _, err := client.Health().Service(serviceName, "", true, nil)
	if err != nil {
		return nil, err
	}
	nodes := []*rdiscovery.ServiceNode{}
	for _, service := range services {
		node := &rdiscovery.ServiceNode{
			Name:    service.Service.Service,
			ID:      service.Service.ID,
			Address: service.Service.Address,
			Port:    service.Service.Port,
		}
		nodes = append(nodes, node)

	}

	// watch serivice
	go func(serviceName string) {
		if err := c.Watch(serviceName); err != nil {
			// FIXME: add retry handle
		}
	}(serviceName)

	return nodes, nil
}

func (c *ConsulRdicovery) Watch(serviceName string) error {
	c.watchLock.Lock()
	if _, ok := c.watchService[serviceName]; ok {
		c.watchService[serviceName] = struct{}{}
		// already watch
		return nil
	}
	c.watchService[serviceName] = struct{}{}
	c.watchLock.Unlock()

	client, err := c.Client()
	if err != nil {
		return err
	}
	wp, err := watch.Parse(map[string]interface{}{
		"type":    "service",
		"service": serviceName,
	})
	if err != nil {
		return err
	}
	wp.Handler = c.watchServiceHandler
	return wp.RunWithClientAndLogger(client, log.New(os.Stderr, "", log.LstdFlags))
}

func (c *ConsulRdicovery) watchServiceHandler(index uint64, res interface{}) {
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
	c.cache.Set(serviceName, nodes)
}

func (c *ConsulRdicovery) watchAllServiceHandler(index uint64, res interface{}) {
	services, ok := res.(map[string][]string)
	if !ok {
		return
	}
	c.watchLock.Lock()
	for serviceName := range c.watchService {
		if _, ok := services[serviceName]; !ok {
			// remove unhealth service
			c.cache.Del(serviceName)
		}
	}
	c.watchLock.Unlock()
}

func (c *ConsulRdicovery) WatchAllService() error {
	client, err := c.Client()
	if err != nil {
		return nil
	}
	wp, err := watch.Parse(map[string]interface{}{
		"type": "services",
	})
	if err != nil {
		return err
	}
	// wp.HybridHandler = c.watchServiceHybridHandler
	wp.Handler = c.watchAllServiceHandler
	return wp.RunWithClientAndLogger(client, log.New(os.Stderr, "", log.LstdFlags))
}

func (c *ConsulRdicovery) Client() (*consulapi.Client, error) {
	if c.client != nil {
		return c.client, nil
	}
	var e error
	config := &consulapi.Config{}
	if c.config != nil {
		config = c.config
	}
	for _, addr := range c.Address {
		config.Address = addr
		client, err := consulapi.NewClient(config)
		if err != nil {
			e = err
			continue
		}
		if _, err := client.Agent().Host(); err != nil {
			e = err
			continue
		}
		c.client = client
		break
	}
	if c.client == nil {
		return nil, fmt.Errorf("no usable address:%s", e)
	}
	return c.client, e
}

func getAgentServiceCheckByOpt(opt *rdiscovery.Options) *consulapi.AgentServiceCheck {
	if opt == nil {
		return nil
	}
	check := &consulapi.AgentServiceCheck{
		Interval: opt.CheckInterval.String(),
		Timeout:  opt.CheckTimeout.String(),
	}

	if _, err := net.ResolveTCPAddr("", opt.CheckAddress); err != nil {
		check.HTTP = opt.CheckAddress
	} else {
		check.TCP = opt.CheckAddress
	}

	return check
}

// func (c *ConsulRdicovery) GetAll() map[string][]*rdiscovery.ServiceNode {
// 	return c.cache.GetAll()
// }
