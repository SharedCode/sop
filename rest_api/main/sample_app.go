package main

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/SharedCode/sop"
	cas "github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/in_red_cfs"
	"github.com/SharedCode/sop/in_red_cfs/fs"
	"github.com/SharedCode/sop/redis"

	"github.com/SharedCode/sop/rest_api"
)

const (
	objectsStore = "objects"
)

// Cassandra Config, please update with your Cassandra Server cluster config.
var cassConfig = cas.Config{
	ClusterHosts: []string{"localhost:9042"},
	Keyspace:     "btree",
}
// Regis Config, please update with your Redis cluster config.
var redisConfig = redis.Options{
	Address:                  "localhost:6379",
	Password:                 "", // no password set
	DB:                       0,  // use default DB
	DefaultDurationInSeconds: 24 * 60 * 60,
}

var ctx = context.TODO();

func init() {
	in_red_cfs.Initialize(cassConfig, redisConfig)

	// Create Stores to ensure we have the sample "objects" btree created in SOP db.
	if err := createStores(); err != nil {
		log.Fatal(err)
		return
	}
}

// Create the "objects" btree store.
func createStores() error {
	trans, err := in_red_cfs.NewTransactionWithEC(sop.ForWriting, -1, false, &fs.ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			// Mimick having paths to three different disks.
			"/Users/grecinto/sop_data/disk1",
			"/Users/grecinto/sop_data/disk2",
			"/Users/grecinto/sop_data/disk3",
		},
		RepairCorruptedShards: false,
	})
	if err != nil {
		return err
	}
	if err = trans.Begin(); err != nil {
		return err
	}

	// Just ensure we have "objects" store created in SOP db.
	_, err = in_red_cfs.NewBtreeWithEC[string, []byte](ctx, sop.StoreOptions{
		Name:                     objectsStore,
		SlotLength:               200,
		IsUnique: true,
		IsValueDataGloballyCached: true,
	}, trans, cmp.Compare)
	if err != nil {
		return err
	}

	// You can add here other create script(s) for other Stores you need in your application...

	trans.Commit(ctx)
	return nil
}

// Register "objects" CRUD REST Api methods.
func registerStores() {
	rest_api.RegisterMethod(rest_api.POST, "/storeitems/:key/:value", addItem)
	rest_api.RegisterMethod(rest_api.GET, "/storeitems/:key", getByKey)

}

// getByKey godoc
// @Summary getByKey returns an item from the store with a given key. 
// @Schemes
// @Description getByKey responds with the details of the matching item as JSON.
// @Tags StoreItems
// @Accept json
// @Produce json
// @Param			key	path		string		true	"Name of item to fetch"    minlength(1)  maxlength(150)
// @Failure 404 {object} map[string]any
// @Success 200 {object} []byte
// @Router /storeitems/{key} [get]
// @Security Bearer
func getByKey(c *gin.Context) {
	itemKey := c.Param("key")

	trans, err := in_red_cfs.NewTransaction(sop.ForReading, -1, false)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "new transaction call in fetching an item failed"})
	}
	if err := trans.Begin(); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("trans.begin failed, error: %v", err)})
		return
	}

	// Ensure to commit the transaction before going out of scope.
	defer trans.Commit(c)

	b3,err :=in_red_cfs.OpenBtree[string, []byte](c, objectsStore, trans, cmp.Compare)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("opening store %s failed, error: %v", objectsStore, err)})
		return
	}

	var found bool
	if found, err = b3.FindOne(c, itemKey, false); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("fetching item %s failed, error: %v", itemKey, err)})
		return
	}
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("item with key %s not found", itemKey)})
		return
	}

	ba, err := b3.GetCurrentValue(c)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("fetching value of item with key %s failed, error: %v", itemKey, err)})
		return
	}
	c.JSON(http.StatusOK, ba)
}

// addItem godoc
// @Summary addItem adds an item to the store with a given key & value pair. 
// @Schemes
// @Description addItem adds an item to the store with a given key & value pair received from POST.
// @Tags StoreItems
// @Accept json
// @Produce json
// @Param			key	path		string		true	"Key of item to add"    minlength(1)  maxlength(150)
// @Param			value path		[]byte]		true	"Value of item to add"    min(1)  max(2000000000)
// @Failure 404 {object} map[string]any
// @Success 200
// @Router /storeitems/{key} [get]
// @Security Bearer
func addItem(c *gin.Context) {
	itemKey := c.Param("key")
	itemValue := c.Param("value")

	trans, err := in_red_cfs.NewTransaction(sop.ForReading, -1, false)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "new transaction call in fetching an item failed"})
	}
	if err := trans.Begin(); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("trans.begin failed, error: %v", err)})
		return
	}

	b3,err :=in_red_cfs.OpenBtree[string, []byte](c, objectsStore, trans, cmp.Compare)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("opening store %s failed, error: %v", objectsStore, err)})
		trans.Rollback(c)
		return
	}

	var found bool
	if found, err = b3.FindOne(c, itemKey, false); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("fetching item %s failed, error: %v", itemKey, err)})
		trans.Rollback(c)
		return
	}
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("item with key %s not found", itemKey)})
		trans.Rollback(c)
		return
	}

	ok, err := b3.UpdateCurrentItem(c, []byte(itemValue))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("update value of item with key %s failed, error: %v", itemKey, err)})
		trans.Rollback(c)
		return
	}
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("update value of item with key %s failed", itemKey)})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("update value of item with key %s succeeded", itemKey)})
	trans.Commit(c)
}
