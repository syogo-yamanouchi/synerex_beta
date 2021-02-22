package main

import (
	"log/syslog"
	"time"

	"github.com/rcrowley/go-metrics"
)

func InitMetricsLog() {
	w, _ := syslog.Dial("unixgram", "/dev/log", syslog.LOG_INFO, "synerex_metrics")
	go metrics.Syslog(metrics.DefaultRegistry, 60*time.Second, w)
}
