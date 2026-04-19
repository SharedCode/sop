package main
import (
"fmt"
"os"
)
func main() {
_, err := os.Stat("config.json")
if err != nil {
fmt.Println("Error:", err)
} else {
fmt.Println("config.json found in CWD")
}
}
