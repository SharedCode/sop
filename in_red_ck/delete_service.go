package in_red_ck

import (
	"context"
	"time"

	log "log/slog"

	"github.com/SharedCode/sop/btree"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/kafka"
)

var lastDeleteTime int64

// Service interval defaults to 2 hours. That is, process deleted items every two hours.
var ServiceIntervalInHour int = 2

// Enable the delete service(defaults to false) if you want this host to poll every periodically to process
// deleted unused Nodes leftover of transaction, the Inactive nodes used as "for merging" node(s).
var IsDeleteServiceEnabled bool

// Sets the Delete Service to enabled(yes = true) or disabled(yes = false).
func EnableDeleteService(yes bool) {
	IsDeleteServiceEnabled = yes
}

// DeleteService runs the DoDeleteItemsProcessing function below periodically, like every 2 hours(default).
func DeleteService(ctx context.Context) {
	// Do nothing if delete service is disabled.
	if !IsDeleteServiceEnabled {
		return
	}
	// Enfore minimum of hourly interval, as deletes processing is not a priority operation.
	// SOP can do without it. It just prevents DB growth size, & nothing critical.
	if ServiceIntervalInHour < 1 {
		ServiceIntervalInHour = 1
	}
	nextRunTime := time.Now().Add(time.Duration(-ServiceIntervalInHour) * time.Hour).UnixMilli()
	if lastDeleteTime < nextRunTime {
		DoDeletedItemsProcessing(ctx)
		lastDeleteTime = time.Now().UnixMilli()
	}
}

// Process(issue delete SQL stmt) the deleted items from the kafka queue.
func DoDeletedItemsProcessing(ctx context.Context) {
	if !kafka.IsInitialized() {
		log.Warn("Kafka is not initialized, please set valid brokers & topic to initialize.")
		return
	}
	blobsIds, err := kafka.Dequeue[[]cas.BlobsPayload[btree.UUID]](ctx, 5)
	if err != nil {
		log.Error("Error kafka dequeue, details: %v", err)
		if len(blobsIds) == 0 {
			return
		}
	}
	bs := cas.NewBlobStore()
	for i := range blobsIds {
		if err := bs.Remove(ctx, blobsIds[i]...); err != nil {
			log.Error("Error removing blobs from Cassandra blobs table, details: %v", err)
		}
	}
}
