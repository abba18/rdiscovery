module github.com/abba18/rdiscovery-plugin/consul

go 1.13

require (
	github.com/abba18/rdiscovery v0.0.0-20200215112251-1e9f6311f78c
	github.com/hashicorp/consul/api v1.4.0
	github.com/hashicorp/go-hclog v0.12.0
)

replace github.com/abba18/rdiscovery v0.0.0-20200215112251-1e9f6311f78c => ../
