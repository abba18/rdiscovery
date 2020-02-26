package consul

import (
	"net"
	"sync"

	"github.com/abba18/rdiscovery"
	consulapi "github.com/hashicorp/consul/api"
)

type ConsulRdicovery struct {
	// common part
	Address string
	client  *consulapi.Client
	config  *consulapi.Config
	closed  bool

	// discovery side part
	EnableCache bool
	cache       rdiscovery.Cache
	watcher     *Watcher

	// register side part
	register     map[string]*rdiscovery.ServiceNode
	registerLock sync.Mutex
}

func NewConsulRdiscovery(address string, cfg *consulapi.Config, c rdiscovery.Cache) (rdiscovery.Register, error) {
	reg := &ConsulRdicovery{
		Address:     address,
		closed:      false,
		EnableCache: false,
		config:      cfg,
		register:    make(map[string]*rdiscovery.ServiceNode),
	}
	if c != nil {
		reg.cache = rdiscovery.NewRCache()
		reg.EnableCache = true
	}
	client, err := reg.Client()
	if err != nil {
		return nil, err
	}

	if reg.EnableCache {
		reg.watcher = NewWatcher(client, reg.cache)
	}

	return reg, nil
}

func (c *ConsulRdicovery) Register(node *rdiscovery.ServiceNode, opt *rdiscovery.Options) error {
	client, err := c.Client()
	if err != nil {
		return err
	}

	check := getAgentServiceCheckByOpt(opt)

	asr := &consulapi.AgentServiceRegistration{
		ID:      node.ID,
		Name:    node.Name,
		Address: node.Address,
		Port:    node.Port,
		Check:   check,
	}
	c.registerLock.Lock()
	defer c.registerLock.Unlock()
	if c.closed {
		return rdiscovery.ErrClose
	}
	if _, ok := c.register[node.Name]; ok {
		return rdiscovery.ErrAlreadyRegitered
	}
	if err := client.Agent().ServiceRegister(asr); err != nil {
		return err
	}
	c.register[node.Name] = node
	return nil
}
func (c *ConsulRdicovery) Deregister(node *rdiscovery.ServiceNode) error {
	client, err := c.Client()
	if err != nil {
		return err
	}
	c.registerLock.Lock()
	defer c.registerLock.Unlock()
	if _, ok := c.register[node.Name]; ok {
		client.Agent().ServiceDeregister(node.ID)
	}
	return nil
}

func (c *ConsulRdicovery) GetService(serviceName string) ([]*rdiscovery.ServiceNode, error) {
	client, err := c.Client()
	if err != nil {
		return nil, err
	}

	// cache first
	if c.EnableCache {
		if nodes, ok := c.cache.Get(serviceName); ok {
			return nodes, nil
		}
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

	if c.EnableCache {
		// watch serivice
		go func(serviceName string) {
			c.watcher.Watch(serviceName)
		}(serviceName)
	}

	return nodes, nil
}

func (c *ConsulRdicovery) Close() {
	c.closed = true
	if c.EnableCache {
		c.watcher.Stop()
	}

	dereg := map[string]*rdiscovery.ServiceNode{}
	c.registerLock.Lock()
	for k, v := range c.register {
		// can not call c.Deregister here,will be dead lock!
		dereg[k] = v
	}
	c.registerLock.Unlock()
	for _, node := range dereg {
		c.Deregister(node)
	}
}

func (c *ConsulRdicovery) Client() (*consulapi.Client, error) {
	if c.client != nil {
		return c.client, nil
	}
	config := &consulapi.Config{}
	if c.config != nil {
		config = c.config
	}
	config.Address = c.Address
	client, err := consulapi.NewClient(config)
	if err != nil {
		return nil, err
	}
	if _, err := client.Agent().Host(); err != nil {
		return nil, err
	}
	c.client = client
	return c.client, nil
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
