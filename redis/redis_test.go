package redis

import (
	"context"
	"fmt"
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
	option := DefaultOptions()
	OpenConnection(option)
	defer CloseConnection()

	c := NewClient()

	ctx := context.Background()
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
		t.Errorf(err.Error())
	}

	_, err = c.GetStruct(ctx, "fooBar", &user)
	if err == nil {
		t.Error("Struct foo still exists after delete.")
	}
}
