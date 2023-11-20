package cache

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
	c := NewClient(option)

	ctx := context.Background()
	item, _ := c.Get(ctx, "key")

	fmt.Println(item)

	usr := user{
		Username:  "foo",
		MobileID:  123,
		Email:     "gy",
		FirstName: "ger",
		LastName:  "rec",
	}

	c.SetStruct(ctx, "fooBar", usr, 0)
	usr2, err := c.GetStruct(ctx, "fooBar", &user{})
	if err != nil {
		t.Error("Struct foo NOT exists.")
	}
	fmt.Println(usr2)

	err = c.Delete(ctx, "fooBar")

	if err != nil {
		t.Errorf(err.Error())
	}

	usr2, err = c.GetStruct(ctx, "fooBar", &user{})
	if err == nil {
		t.Error("Struct foo still exists after delete.")
	}

}
