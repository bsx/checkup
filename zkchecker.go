package checkup

import (
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// ZkChecker implements a Checker for Zookeeper.
type ZkChecker struct {
	// Name is the name of the endpoint.
	Name     string `json:"endpoint_name"`

	// URL is the URL of the endpoint.
	Servers  []string `json:"servers"`

	// Timeout is the maximum time to wait for a
	// TCP connection to be established.
	Timeout  time.Duration `json:"timeout,omitempty"`

	// Attempts is how many requests the client will
	// make to the endpoint in a single check.
	Detailed bool `json:"detailed,omitempty"`

	// ThresholdRTT is the maximum round trip time to
	// allow for a healthy endpoint. If non-zero and a
	// request takes longer than ThresholdRTT, the
	// endpoint will be considered unhealthy. Note that
	// this duration includes any in-between network
	// latency.
	ThresholdRTT time.Duration `json:"threshold_rtt,omitempty"`

	// Attempts is how many requests the client will
	// make to the endpoint in a single check.
	Attempts int `json:"attempts,omitempty"`
}

// Check performs checks using c according to its configuration.
// An error is only returned if there is a configuration error.
func (c ZkChecker) Check() (Result, error) {
	if c.Attempts < 1 {
		c.Attempts = 1
	}

	result := Result{Title: c.Name, Timestamp: Timestamp()}
	result.Times = c.doChecks()

	return c.conclude(result), nil
}

// doChecks executes and returns each attempt.
func (c ZkChecker) doChecks() Attempts {
	var err error
	var ok bool

	timeout := c.Timeout
	if timeout == 0 {
		timeout = 1 * time.Second
	}

	checks := make(Attempts, c.Attempts)
	for i := 0; i < c.Attempts; i++ {
		start := time.Now()

		if c.Detailed {
			var results []*zk.ServerStats
			var followers, leaders int
			standalone := false
			results, ok = zk.FLWSrvr(c.Servers, timeout)
			for _, stat := range results {
				switch stat.Mode {
				case zk.ModeFollower:
					followers++
				case zk.ModeLeader:
					leaders++
				case zk.ModeUnknown:
					ok = false
				case zk.ModeStandalone:
					ok = ok && len(results) == 1
					standalone = true
				}
			}
			if !standalone {
				ok = ok && leaders == 1 && leaders+followers == len(results)
			}
		} else {
			results := zk.FLWRuok(c.Servers, timeout)
			ok = true
			for _, imok := range results {
				ok = ok && imok
			}
		}

		if !ok {
			checks[i].Error = "one or more nodes reported errors"
		}

		checks[i].RTT = time.Since(start)
		if err != nil {
			checks[i].Error = err.Error()
			continue
		}
	}
	return checks
}

// conclude takes the data in result from the attempts and
// computes remaining values needed to fill out the result.
// It detects degraded (high-latency) responses and makes
// the conclusion about the result's status.
func (c ZkChecker) conclude(result Result) Result {
	result.ThresholdRTT = c.ThresholdRTT

	// Check errors (down)
	for i := range result.Times {
		if result.Times[i].Error != "" {
			result.Down = true
			return result
		}
	}

	// Check round trip time (degraded)
	if c.ThresholdRTT > 0 {
		stats := result.ComputeStats()
		if stats.Median > c.ThresholdRTT {
			result.Notice = fmt.Sprintf("median round trip time exceeded threshold (%s)", c.ThresholdRTT)
			result.Degraded = true
			return result
		}
	}

	result.Healthy = true
	return result
}
