package main

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_cfs"
)

type storesRestApi struct{}

func NewStoresRestApi() *storesRestApi {
	return &storesRestApi{}
}

// GetStores godoc
// @Summary GetStores returns list of stores
// @Schemes
// @Description GetStores responds with the list of all stores as JSON.
// @Tags Jobs
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
