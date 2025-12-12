package redis

import (
	"context"
	"fmt"
	"os"
	"testing"
)

type user struct {
	Username  string `json:"username"`
	MobileID  int    `json:"mobile_id"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

func TestBasicUse(t *testing.T) {
	if os.Getenv("SOP_REDIS_TEST") != "1" {
		t.Skip("skipping Redis integration test; set SOP_REDIS_TEST=1 to run")
	}

	option := DefaultOptions()
	OpenConnection(option)
	defer CloseConnection()

	c := NewClient()

	ctx := context.Background()
	if err := c.Ping(ctx); err != nil {
		t.Skipf("skipping Redis integration test; Redis not reachable: %v", err)
	}
	_, item, _ := c.Get(ctx, "key")

	fmt.Println(item)

	usr := user{
		Username:  "foo",
		MobileID:  123,
		Email:     "gy",
		FirstName: "ger",
		LastName:  "rec",
	}

	c.SetStruct(ctx, "fooBar", &usr, 0)
	user := user{}
	_, err := c.GetStruct(ctx, "fooBar", &user)
	if err != nil {
		t.Error("Struct foo NOT exists.")
	}
	fmt.Println(user)

	_, err = c.Delete(ctx, []string{"fooBar"})
	if err != nil {
		t.Errorf("%v", err)
	}

	found, err := c.GetStruct(ctx, "fooBar", &user)
	if found {
		t.Error("Struct foo still exists after delete.")
	}
}
