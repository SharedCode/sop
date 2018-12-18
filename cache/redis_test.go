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
	usr2, err := c.GetStruct("fooBar", &user{})
	if err != nil{
		t.Error("Struct foo NOT exists.")
	}
	fmt.Println(usr2)

	err = c.DeleteStruct("fooBar")

	if err != nil{
		t.Errorf(err.Error())
	}

	usr2, err = c.GetStruct("fooBar", &user{})
	if err == nil{
		t.Error("Struct foo still exists after delete.")
	}

}
