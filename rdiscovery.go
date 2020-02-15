package rdiscovery

import "time"

type Register interface {
	Register(Node *ServiceNode, opt *Options) error
	Deregister(Node *ServiceNode) error
	GetService(service string) ([]*ServiceNode, error)
}

type Cache interface {
	Get(service string) ([]*ServiceNode, bool)
	Set(service string, nodes []*ServiceNode)
	Del(service string)
}

type ServiceNode struct {
	Name    string
	ID      string
	Address string
	Port    int
}

type Options struct {
	CheckInterval time.Duration
	CheckTimeout  time.Duration
	CheckAddress  string
}
