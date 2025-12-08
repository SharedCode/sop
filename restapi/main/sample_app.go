package main

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"

	"github.com/sharedcode/sop"
	cas "github.com/sharedcode/sop/adapters/cassandra"
	"github.com/sharedcode/sop/adapters/redis"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/infs"

	"github.com/sharedcode/sop/restapi"
)

const (
	objectsStore = "objects"
)

var dataPath = "/tmp/sop_data"

// Cassandra Config, please update with your Cassandra Server cluster config.
var cassConfig = cas.Config{
	ClusterHosts: []string{"localhost:9042"},
	Keyspace:     "btree",
}

// Regis Config, please update with your Redis cluster config.
var redisConfig = redis.Options{
	Address:  "localhost:6379",
	Password: "", // no password set
	DB:       0,  // use default DB
}

var ctx = context.TODO()
var ecConfig map[string]sop.ErasureCodingConfig

func init() {
	if dp := os.Getenv("datapath"); dp != "" {
		dataPath = dp
	}
	if _, err := cas.OpenConnection(cassConfig); err != nil {
		log.Fatal(err)
	}
	if _, err := redis.OpenConnection(redisConfig); err != nil {
		log.Fatal(err)
	}

	// Initialize EC config
	ecConfig = make(map[string]sop.ErasureCodingConfig)
	ecConfig[""] = sop.ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			// Mimick having paths to three different disks.
			fmt.Sprintf("%s/disk1", dataPath),
			fmt.Sprintf("%s/disk2", dataPath),
			fmt.Sprintf("%s/disk3", dataPath),
		},
		RepairCorruptedShards: false,
	}

	// Initialize Database
	var err error
	restapi.DB, err = database.ValidateOptions(sop.DatabaseOptions{
		ErasureConfig: ecConfig,
		StoresFolders: ecConfig[""].BaseFolderPathsAcrossDrives,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Create Stores to ensure we have the sample "objects" btree created in SOP db.
	if err := createStores(); err != nil {
		log.Fatal(err)
		return
	}
}

// Create the "objects" btree store.
func createStores() error {
	trans, err := database.BeginTransaction(ctx, restapi.DB, sop.ForWriting)
	if err != nil {
		return err
	}
	// BeginTransaction already calls Begin if successful?
	// Let's check database.go.
	// Yes: "if err := t.Begin(ctx); err != nil { return nil, err }"
	// So I don't need to call Begin again.
	// But wait, BeginTransaction returns (sop.Transaction, error).
	// Does it return an already begun transaction?
	// Yes.

	// Just ensure we have "objects" store created in SOP db.
	_, err = infs.NewBtreeWithReplication[string, []byte](ctx, sop.StoreOptions{
		Name:                      objectsStore,
		SlotLength:                200,
		IsUnique:                  true,
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
	restapi.RegisterMethod(restapi.POST, "/storeitems/:key/:value", addItem)
	restapi.RegisterMethod(restapi.GET, "/storeitems/:key", getByKey)

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

	trans, err := database.BeginTransaction(c, restapi.DB, sop.ForReading)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "new transaction options call failed"})
		return
	}

	// Ensure to commit the transaction before going out of scope.
	defer trans.Commit(c)

	b3, err := infs.OpenBtreeWithReplication[string, []byte](c, objectsStore, trans, cmp.Compare)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("opening store %s failed, error: %v", objectsStore, err)})
		return
	}

	var found bool
	if found, err = b3.Find(c, itemKey, false); err != nil {
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

	trans, err := database.BeginTransaction(c, restapi.DB, sop.ForWriting)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "new transaction options call failed"})
		return
	}

	b3, err := infs.OpenBtreeWithReplication[string, []byte](c, objectsStore, trans, cmp.Compare)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("opening store %s failed, error: %v", objectsStore, err)})
		if rbErr := trans.Rollback(c); rbErr != nil {
			log.Printf("Rollback failed: %v", rbErr)
		}
		return
	}

	var found bool
	if found, err = b3.Find(c, itemKey, false); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("fetching item %s failed, error: %v", itemKey, err)})
		if rbErr := trans.Rollback(c); rbErr != nil {
			log.Printf("Rollback failed: %v", rbErr)
		}
		return
	}
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("item with key %s not found", itemKey)})
		if rbErr := trans.Rollback(c); rbErr != nil {
			log.Printf("Rollback failed: %v", rbErr)
		}
		return
	}

	ok, err := b3.UpdateCurrentValue(c, []byte(itemValue))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("update value of item with key %s failed, error: %v", itemKey, err)})
		if rbErr := trans.Rollback(c); rbErr != nil {
			log.Printf("Rollback failed: %v", rbErr)
		}
		return
	}
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("update value of item with key %s failed", itemKey)})
		if rbErr := trans.Rollback(c); rbErr != nil {
			log.Printf("Rollback failed: %v", rbErr)
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("update value of item with key %s succeeded", itemKey)})
	trans.Commit(c)
}
