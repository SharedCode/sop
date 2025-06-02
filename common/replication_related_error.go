package common

// Error related to replication. IO writers/readers can raise this kind of error and
// Transaction manager will handle this kind of error to implement replication related events
// and actions, e.g. failover or recover (opposite of failover).
type ReplicationRelatedError struct {
	Err error
}

func (r ReplicationRelatedError) Error() string {
	return r.Err.Error()
}
