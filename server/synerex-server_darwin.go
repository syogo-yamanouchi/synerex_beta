package main

import (
	"log"
	"time"

	metrics "github.com/rcrowley/go-metrics"
)

//func InitMetricsLog() {
//	w, _ := syslog.Dial("unixgram", "/dev/log", syslog.LOG_INFO, "synerex_metrics")
//	go metrics.Syslog(metrics.DefaultRegistry, 60*time.Second, w)
//}

func periodicLog(r metrics.Registry, freq time.Duration) {
	for _ = range time.Tick(freq) {
		var total, receive, send int64
		r.Each(func(name string, i interface{}) {
			switch metric := i.(type) {
			case metrics.Counter:
				//				l.Printf("counter %s count:\t %9d\n", name, metric.Count())
				if name == "messages.total" {
					total = metric.Count()
				}
				if name == "messages.receive" {
					receive = metric.Count()
				}
				if name == "messages.send" {
					send = metric.Count()
				}
			}
		})
		log.Printf("metric total: %7d :send: %7d :recv: %7d", total, send, receive)
	}
}

func InitMetricsLog() {
	//	log.Printf("Metric log for Windows started.")
	go periodicLog(metrics.DefaultRegistry, 60*time.Second)
}
