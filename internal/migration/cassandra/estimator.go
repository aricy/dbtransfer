package cassandra

import (
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/sirupsen/logrus"

	"dbtransfer/internal/i18n"
)

// TotalRowEstimator defines the interface for estimating total row count in a table
type TotalRowEstimator interface {
	// EstimateTotalRows returns an estimated total row count for the given keyspace and table
	// The returned isEstimated flag indicates if the count is an approximation (true) or exact (false)
	EstimateTotalRows(keyspace, table string) (int64, bool, error)
	// GetAvgPartitionSize returns the last cached average partition size in bytes
	GetAvgPartitionSize() int64
}

// SizeEstimatesEstimator uses Cassandra's system.size_estimates table to estimate row count
// Compatible with: Cassandra 3.x+, ScyllaDB 4.x+
// Queries all nodes in the cluster and sums partition counts from each node
type SizeEstimatesEstimator struct {
	session         *gocql.Session
	cluster         *gocql.ClusterConfig
	username        string
	password        string
	hosts           []string
	lastAvgPartSize int64
	lastTable       string
}

// NewSizeEstimatesEstimator creates a new SizeEstimatesEstimator using the provided Cassandra session
func NewSizeEstimatesEstimator(session *gocql.Session) *SizeEstimatesEstimator {
	return &SizeEstimatesEstimator{session: session}
}

// NewSizeEstimatesEstimatorWithCluster creates a new SizeEstimatesEstimator with cluster config
// used for querying multiple nodes
func NewSizeEstimatesEstimatorWithCluster(cluster *gocql.ClusterConfig) *SizeEstimatesEstimator {
	return &SizeEstimatesEstimator{cluster: cluster}
}

// NewSizeEstimatesEstimatorWithAuth creates a new SizeEstimatesEstimator with authentication
// and source hosts for direct system queries (avoids TokenAwareHostPolicy routing issues).
func NewSizeEstimatesEstimatorWithAuth(session *gocql.Session, hosts []string, username, password string) *SizeEstimatesEstimator {
	return &SizeEstimatesEstimator{session: session, username: username, password: password, hosts: hosts}
}

// EstimateTotalRows queries all nodes' system.size_estimates to get total partition count
// across the entire cluster by summing per-node partition counts.
// Note: For ScyllaDB, official bug #9083 may affect accuracy in some scenarios
func (e *SizeEstimatesEstimator) EstimateTotalRows(keyspace, table string) (int64, bool, error) {
	logrus.Debugf("Estimating total rows for %s.%s", keyspace, table)

	// Get all node IPs in the cluster
	hosts, err := e.getClusterHosts()
	if err != nil {
		logrus.Warnf("Failed to get cluster hosts: %v", err)
		return 0, true, fmt.Errorf("failed to get cluster hosts: %w", err)
	}
	logrus.Debugf("Found %d hosts in cluster: %v", len(hosts), hosts)

	// Query each node for its local partition count
	var totalPartitions int64
	var totalSize int64
	nodeDetails := make([]string, 0, len(hosts))
	for _, host := range hosts {
		count, size, err := e.queryNodePartitions(host, keyspace, table)
		if err != nil {
			logrus.Warnf("Failed to query node %s partitions: %v", host, err)
			continue
		}
		logrus.Debugf("Host %s returned partitions count: %d, size: %d", host, count, size)
		totalPartitions += count
		totalSize += size
		nodeDetails = append(nodeDetails, fmt.Sprintf("%s=%d", host, count))
	}

	if totalPartitions == 0 {
		return 0, true, fmt.Errorf("no partition data found for table %s.%s", keyspace, table)
	}

	logrus.Infof(i18n.Tr("分区数估算: 各节点分区数 [%s] -> 总和=%d", "Partition estimation: per-node partitions [%s] -> total=%d"),
		strings.Join(nodeDetails, ", "), totalPartitions)

	// Calculate average partition size (totalSize / totalPartitions) for row estimation
	// Cache it for later use to estimate processed partitions from rows
	if totalPartitions > 0 {
		e.lastAvgPartSize = totalSize / totalPartitions
		e.lastTable = table
	}

	logrus.Debugf("Total partitions: %d, Avg partition size: %d bytes",
		totalPartitions, e.lastAvgPartSize)
	return totalPartitions, true, nil
}

// GetAvgPartitionSize returns the last cached average partition size in bytes
func (e *SizeEstimatesEstimator) GetAvgPartitionSize() int64 {
	return e.lastAvgPartSize
}

// getClusterHosts returns all unique host IPs in the cluster by querying system.peers and system.local
// Uses a direct connection (no TokenAwareHostPolicy) to ensure system.local and system.peers
// are queried from the same coordinator node.
func (e *SizeEstimatesEstimator) getClusterHosts() ([]string, error) {
	// Create a plain session to the first configured host for system discovery queries.
	// TokenAwareHostPolicy can route system.local and system.peers to different coordinators,
	// causing inconsistent results. Use plain RoundRobin to hit the same node.
	var discoverSession *gocql.Session
	if len(e.hosts) > 0 {
		discoverCluster := gocql.NewCluster(e.hosts[0])
		discoverCluster.DisableInitialHostLookup = true // pin to the configured host only
		discoverCluster.Consistency = gocql.One
		discoverCluster.Timeout = 5 * time.Second
		if e.username != "" && e.password != "" {
			discoverCluster.Authenticator = gocql.PasswordAuthenticator{
				Username: e.username,
				Password: e.password,
			}
		}
		var err error
		discoverSession, err = discoverCluster.CreateSession()
		if err != nil {
			logrus.Warnf("Failed to create discovery session: %v, falling back to main session", err)
			discoverSession = e.session
		} else {
			defer discoverSession.Close()
		}
	} else {
		discoverSession = e.session
	}

	seen := make(map[string]bool)
	var hosts []string

	// First get the current node
	var localHost string
	err := discoverSession.Query(`SELECT listen_address FROM system.local`).Scan(&localHost)
	if err == nil && localHost != "" && !seen[localHost] {
		seen[localHost] = true
		hosts = append(hosts, localHost)
	} else if err != nil {
		logrus.Warnf("Failed to get local host: %v", err)
	}

	// Then get all peer nodes
	iter := discoverSession.Query(`SELECT peer FROM system.peers`).Iter()
	var peer string
	for iter.Scan(&peer) {
		if peer != "" && !seen[peer] {
			seen[peer] = true
			hosts = append(hosts, peer)
		}
	}
	if err := iter.Close(); err != nil {
		logrus.Warnf(i18n.Tr("关闭节点迭代器失败: %v", "Failed to close peers iterator: %v"), err)
	}

	logrus.Infof(i18n.Tr("集群节点发现: %d 个唯一节点 %v", "Cluster node discovery: %d unique nodes %v"), len(hosts), hosts)

	if len(hosts) == 0 {
		return nil, fmt.Errorf("no hosts found in cluster")
	}

	return hosts, nil
}

// queryNodePartitions queries a specific node's system.size_estimates for partition count
// and total size. Returns (totalPartitions, totalSizeBytes, error)
func (e *SizeEstimatesEstimator) queryNodePartitions(host, keyspace, table string) (int64, int64, error) {
	// Create a dedicated session to the specific node
	nodeConfig := gocql.NewCluster(host)
	nodeConfig.DisableInitialHostLookup = true
	nodeConfig.Consistency = gocql.One
	nodeConfig.Timeout = 5 * time.Second

	// Use authentication if provided
	if e.username != "" && e.password != "" {
		nodeConfig.Authenticator = gocql.PasswordAuthenticator{
			Username: e.username,
			Password: e.password,
		}
	}

	session, err := nodeConfig.CreateSession()
	if err != nil {
		return 0, 0, err
	}
	defer session.Close()

	// Query partition counts and sizes for this table on this node and sum them
	// Note: mean_partition_size may be a cluster-wide average, not per-node
	query := `SELECT partitions_count, mean_partition_size
			  FROM system.size_estimates
			  WHERE keyspace_name = ? AND table_name = ?`

	var totalCount int64
	var totalSize int64
	iter := session.Query(query, keyspace, table).PageSize(5000).Iter()
	var count int64
	var meanSize int64
	for iter.Scan(&count, &meanSize) {
		totalCount += count
		totalSize += count * meanSize // Estimate total size = partitions * avg size
	}
	if err := iter.Close(); err != nil {
		return 0, 0, err
	}

	return totalCount, totalSize, nil
}

// NoOpEstimator is a fallback estimator that returns 0 (unknown total)
// Use when size_estimates is unavailable or estimation fails
type NoOpEstimator struct{}

// EstimateTotalRows always returns 0, false (not estimated, but unknown)
func (e *NoOpEstimator) EstimateTotalRows(keyspace, table string) (int64, bool, error) {
	return 0, false, nil
}

// GetAvgPartitionSize returns 0 for NoOpEstimator
func (e *NoOpEstimator) GetAvgPartitionSize() int64 {
	return 0
}

// RateLimiterStats tracks rate limiting statistics to correct TPS calculation
// Fixes TPS misalignment where wait time during EnforceGlobalRateLimit() was not counted
type RateLimiterStats struct {
	// Cumulative wait time spent in rate limiter
	TotalWaitTime time.Duration
	// Number of times rate limiting was enforced
	WaitCount int64
	// Number of batches processed
	BatchCount int64
	// Total rows processed
	TotalRows int64
}

// RecordWait records a rate limiting wait period
func (r *RateLimiterStats) RecordWait(waitTime time.Duration) {
	r.TotalWaitTime += waitTime
	r.WaitCount++
}

// RecordBatch records a batch completion with its row count
func (r *RateLimiterStats) RecordBatch(rowCount int64) {
	r.BatchCount++
	r.TotalRows += rowCount
}

// EffectiveRate calculates the effective processing rate accounting for rate limiting wait time
// Formula: TotalRows / (WallClockTime + WaitTime)
// This corrects the TPS display to show actual throughput including rate limit delays
func (r *RateLimiterStats) EffectiveRate(wallClockElapsed time.Duration) float64 {
	if wallClockElapsed <= 0 {
		return 0
	}
	// Add wait time to elapsed time for accurate rate calculation
	effectiveElapsed := wallClockElapsed + r.TotalWaitTime
	if effectiveElapsed <= 0 {
		return 0
	}
	return float64(r.TotalRows) / effectiveElapsed.Seconds()
}

// AverageBatchWaitTime returns the average wait time per batch
func (r *RateLimiterStats) AverageBatchWaitTime() time.Duration {
	if r.WaitCount == 0 {
		return 0
	}
	return r.TotalWaitTime / time.Duration(r.WaitCount)
}

// Reset clears all statistics (for testing or new migration)
func (r *RateLimiterStats) Reset() {
	r.TotalWaitTime = 0
	r.WaitCount = 0
	r.BatchCount = 0
	r.TotalRows = 0
}