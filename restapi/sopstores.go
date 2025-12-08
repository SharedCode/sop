package restapi

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/infs"
)

// DB is the database instance.
var DB sop.DatabaseOptions

// DataPath is the path to the data directory.
var DataPath string

// GetStores godoc
// @Summary GetStores returns list of stores
// @Schemes
// @Description GetStores responds with the list of all stores as JSON.
// @Tags Stores
// @Accept json
// @Produce json
// @Failure 404 {object} map[string]any
// @Success 200 {object} []string
// @Router /stores [get]
// @Security Bearer
func GetStores(c *gin.Context) {
	trans, err := database.BeginTransaction(c, DB, sop.ForWriting)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "new transaction call in fetching stores list failed"})
		return
	}
	stores, err := trans.GetStores(c)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "fetching stores list failed"})
		return
	}
	c.IndentedJSON(http.StatusOK, stores)
}

// GetStoreByName godoc
// @Summary GetStoreByName returns details of a store having its name matching the name parameter.
// @Schemes
// @Description GetStoreByName responds with the details of the matching store as JSON.
// @Tags Stores
// @Accept json
// @Produce json
// @Param			name	path		string		true	"Name of store to fetch"    minlength(1)  maxlength(20)
// @Failure 404 {object} map[string]any
// @Success 200 {object} sop.StoreInfo
// @Router /stores/{name} [get]
// @Security Bearer
func GetStoreByName(c *gin.Context) {
	storeName := c.Param("name")

	trans, err := database.BeginTransaction(c, DB, sop.ForReading)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "new transaction call in fetching stores list failed"})
		return
	}

	// Just end the transaction, rollback does nothing.
	defer trans.Rollback(c)

	// We assume replication is enabled if DB is configured so.
	b3, err := infs.OpenBtreeWithReplication[interface{}, interface{}](c, storeName, trans, nil)
	if err != nil {
		// Fallback to non-replication if failed (e.g. if DB is not configured for replication)
		b3, err = infs.OpenBtree[interface{}, interface{}](c, storeName, trans, nil)
		if err != nil {
			c.IndentedJSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("fetching store %s failed, error: %v", storeName, err)})
			return
		}
	}

	si := b3.GetStoreInfo()
	c.IndentedJSON(http.StatusOK, si)
}
