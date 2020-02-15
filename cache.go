package rdiscovery

import "sync"

type RCache struct {
	serviceNodes map[string][]*ServiceNode
	sync.RWMutex
}

func NewRCache() *RCache {
	return &RCache{
		serviceNodes: make(map[string][]*ServiceNode),
	}
}

func (c *RCache) Get(service string) ([]*ServiceNode, bool) {
	c.RLock()
	defer c.RUnlock()
	nodes, ok := c.serviceNodes[service]
	return nodes, ok
}
func (c *RCache) Set(service string, nodes []*ServiceNode) {
	c.Lock()
	defer c.Unlock()
	c.serviceNodes[service] = nodes
}
func (c *RCache) Del(service string) {
	c.Lock()
	defer c.Unlock()
	delete(c.serviceNodes, service)
}

func (c *RCache) GetAll() map[string][]*ServiceNode {
	c.RLock()
	c.RUnlock()
	m := make(map[string][]*ServiceNode)
	for k, v := range c.serviceNodes {
		m[k] = v
	}
	return m
}
