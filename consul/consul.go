package consul

import (
	"fmt"
	"net"

	"github.com/abba18/rdiscovery"
	consulapi "github.com/hashicorp/consul/api"
)

type ConsulRdicovery struct {
	// common part
	Address []string
	client  *consulapi.Client
	config  *consulapi.Config

	// discovery side part
	EnableCache bool
	cache       rdiscovery.Cache
	watcher     *Watcher
}

func NewConsulRdiscovery(address []string, cfg *consulapi.Config, c rdiscovery.Cache) (rdiscovery.Register, error) {
	reg := &ConsulRdicovery{
		Address:     address,
		EnableCache: false,
		config:      cfg,
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
