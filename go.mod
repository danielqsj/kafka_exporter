module github.com/davidmparrott/kafka_exporter/v2

go 1.15

require (
	github.com/Shopify/sarama v1.27.2
	github.com/go-kit/kit v0.10.0
	//github.com/grafana/agent v0.11.0
	github.com/krallistic/kazoo-go v0.0.0-20170526135507-a15279744f4e
	github.com/prometheus/client_golang v1.9.0
	github.com/prometheus/common v0.15.0
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
)

//replace (
//	github.com/google/dnsmasq_exporter => github.com/grafana/dnsmasq_exporter v0.2.1-0.20201029182940-e5169b835a23
//	github.com/ncabatoff/process-exporter => github.com/grafana/process-exporter v0.7.3-0.20210106202358-831154072e2a
//	github.com/prometheus/mysqld_exporter => github.com/grafana/mysqld_exporter v0.12.2-0.20201015182516-5ac885b2d38a
//	github.com/wrouesnel/postgres_exporter => github.com/grafana/postgres_exporter v0.8.1-0.20201106170118-5eedee00c1db
//)
//
//replace github.com/prometheus/consul_exporter => github.com/prometheus/consul_exporter v0.7.2-0.20210127095228-584c6de19f23
//replace	k8s.io/client-go => k8s.io/client-go v0.19.2
