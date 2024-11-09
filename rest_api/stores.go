package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_cfs"
)

type storesRestApi struct{}

func NewStoresRestApi() *storesRestApi {
	return &storesRestApi{}
}


/*
const dataPath string = "/Users/grecinto/sop_data"

func RegisterIt() error {

	beginTrans := func(mode sop.TransactionMode, timeOut time.Duration) (sop.Transaction, error) {
		trans, err := in_red_cfs.NewTransactionWithEC(sop.ForWriting, -1, false, &fs.ErasureCodingConfig{
			DataShardsCount:   2,
			ParityShardsCount: 1,
			BaseFolderPathsAcrossDrives: []string{
				fmt.Sprintf("%s%cdisk1", dataPath, os.PathSeparator),
				fmt.Sprintf("%s%cdisk2", dataPath, os.PathSeparator),
				fmt.Sprintf("%s%cdisk3", dataPath, os.PathSeparator),
			},
			RepairCorruptedShards: true,
		})
		if err != nil {
			return nil, err
		}
		if err = trans.Begin(); err != nil {
			return nil, err
		}
		return trans, nil
	}

	b3, err := in_red_cfs.OpenBtree[int, string](ctx, "barstoreec", trans, cmp.Compare)
	

	trans.Commit(ctx)
}
*/

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
func (sra *storesRestApi) GetStores(c *gin.Context) {
	trans, err := in_red_cfs.NewTransaction(sop.ForWriting, -1, false)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "new transaction call in fetching stores list failed"})
	}
	stores, err := trans.GetStores(ctx)
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
func (sra *storesRestApi) GetStoreByName(c *gin.Context) {
	storeName := c.Param("name")

	trans, err := in_red_cfs.NewTransaction(sop.ForReading, -1, false)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "new transaction call in fetching stores list failed"})
	}
	if err := trans.Begin(); err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("trans.begin failed, error: %v", err)})
		return
	}

	// Just end the transaction, rollback does nothing.
	defer trans.Rollback(ctx)

	b3,err :=in_red_cfs.OpenBtree[interface{}, interface{}](ctx, storeName, trans, nil)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("fetching store %s failed, error: %v", storeName, err)})
		return
	}

	si := b3.GetStoreInfo();
	c.IndentedJSON(http.StatusOK, si)
}
