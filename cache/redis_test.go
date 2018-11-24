package cache

import "testing"
import "fmt"

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

	item, _ := c.Get("key")

	fmt.Println(item)

	usr := user{
		Username:"foo",
		MobileID:123,
		Email:"gy",
		FirstName:"ger",
		LastName:"rec",
	}

	c.SetStruct("fooBar", usr, 0)
	usr2, _ := c.GetStruct("fooBar", &user{})

	fmt.Println(usr2)
}
