package restapi

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/inredcfs"
)

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
	trans, err := inredcfs.NewTransaction(sop.ForWriting, -1, false)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "new transaction call in fetching stores list failed"})
	}
	stores, err := trans.GetStores(c)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "fetching stores list failed"})
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

	trans, err := inredcfs.NewTransaction(sop.ForReading, -1, false)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "new transaction call in fetching stores list failed"})
	}
	if err := trans.Begin(); err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("trans.begin failed, error: %v", err)})
		return
	}

	// Just end the transaction, rollback does nothing.
	defer trans.Rollback(c)

	b3, err := inredcfs.OpenBtree[interface{}, interface{}](c, storeName, trans, nil)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("fetching store %s failed, error: %v", storeName, err)})
		return
	}

	si := b3.GetStoreInfo()
	c.IndentedJSON(http.StatusOK, si)
}
