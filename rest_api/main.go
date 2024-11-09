package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	jwtverifier "github.com/okta/okta-jwt-verifier-golang"
	swaggerfiles "github.com/swaggo/files"     // swagger embed files
	ginSwagger "github.com/swaggo/gin-swagger" // gin-swagger middleware

	cas "github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/in_red_cfs"
	"github.com/SharedCode/sop/redis"
	"github.com/SharedCode/sop/rest_api/docs"
)

var ctx = context.TODO()

// USE REST API to surface SOP Transactions & BTrees.
// USE cel-go to support scripting. Example, to provide "comparer" function to SOP's BTree.
// To provide search functionality, etc...
// The solution enables existing REST API tools such as curl, Postman, etc... for data browsing and management!

// @BasePath /api/v1

// See goth package for OAuth2 based authentication: https://github.com/markbates/goth
// See oauth2 token verifier (& VueJS based token injection in Header)
//     sample using Okta: https://developer.okta.com/blog/2021/02/17/building-and-securing-a-go-and-gin-web-application
// See this for how to package token after goth supported provider authentication: https://github.com/markbates/goth/issues/310
// Use this cmd to generate Swagger docs: ~/go/bin/swag init --parseDependency

var toValidate = map[string]string{
	"aud": "api://default",
	"cid": os.Getenv("OKTA_CLIENT_ID"),
}

// Verify the bearer token in header.
func verify(c *gin.Context) bool {
	status := true

	// Allow easy debugging on dev.
	if os.Getenv("SOP_ENV") == "DEV" {
		return true
	}

	token := c.Request.Header.Get("Authorization")
	if strings.HasPrefix(token, "Bearer ") {
		token = strings.TrimPrefix(token, "Bearer ")

		// Allow easy QA, bypass Okta based OAuth2 token verification w/ simple token equality check.
		if os.Getenv("SOP_ENV") == "QA" {
			devToken := os.Getenv("SOP_QA_TOKEN")
			if token == devToken {
				return true
			}
		}

		verifierSetup := jwtverifier.JwtVerifier{
			Issuer:           "https://" + os.Getenv("OKTA_DOMAIN") + "/oauth2/default",
			ClaimsToValidate: toValidate,
		}
		verifier := verifierSetup.New()
		_, err := verifier.VerifyAccessToken(token)
		if err != nil {
			c.String(http.StatusForbidden, err.Error())
			print(err.Error())
			status = false
		}
	} else {
		c.String(http.StatusUnauthorized, "Unauthorized")
		status = false
	}
	return status
}

var cassConfig = cas.Config{
	ClusterHosts: []string{"localhost:9042"},
	Keyspace:     "btree",
}
var redisConfig = redis.Options{
	Address:                  "localhost:6379",
	Password:                 "", // no password set
	DB:                       0,  // use default DB
	DefaultDurationInSeconds: 24 * 60 * 60,
}

func init() {
	in_red_cfs.Initialize(cassConfig, redisConfig)
}

// @securityDefinitions.apikey Bearer
// @in header
// @name Authorization
// @description Type "Bearer" followed by a space and JWT token.
func main() {

	// Simple closure to for header token verification.
	verifyHeaderToken := func(realHandler func(c *gin.Context)) func(c *gin.Context) {
		return func(c *gin.Context) {
			if verify(c) {
				realHandler(c)
			}
		}
	}

	router := gin.Default()
	docs.SwaggerInfo.BasePath = "/api/v1"

	stores := NewStoresRestApi()
	// Register the (main) Stores' REST Api.
	RegisterMethod(GET, "/stores", stores.GetStores)
	RegisterMethod(GET, "/stores/:name", stores.GetStoreByName)

	v1 := router.Group("/api/v1")
	{
		for _, rm := range restMethods {
			switch(rm.Verb) {
			case GET:
				fallthrough
			case GET_ONE:
				v1.GET(rm.Path, verifyHeaderToken(rm.handler))
			case DELETE:
				v1.DELETE(rm.Path, verifyHeaderToken(rm.handler))
			case POST:
				v1.POST(rm.Path, verifyHeaderToken(rm.handler))
			case PUT:
				v1.PUT(rm.Path, verifyHeaderToken(rm.handler))
			case PATCH:
				v1.PATCH(rm.Path, verifyHeaderToken(rm.handler))
			default:
				panic(fmt.Sprintf("HTTP verb %d not supported", rm.Verb))
			}
		}

		/*
		// Add store.
		v1.POST("/stores", verifyHeaderToken(stores.PostStore))
		*/

	}

	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerfiles.Handler))
	router.Run("localhost:8080")
}
