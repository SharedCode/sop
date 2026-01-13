package main

import (
	"cmp"
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/database"
)

// Demo Data Structures

type UserProfile struct {
	ID        string `json:"id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Email     string `json:"email"`
	Country   string `json:"country"`
	Age       int    `json:"age"`
	Gender    string `json:"gender"`
}

type Product struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Category    string  `json:"category"`
	Price       float64 `json:"price"`
	Stock       int     `json:"stock"`
}

type Order struct {
	ID          string      `json:"id"`
	UserID      string      `json:"user_id"`
	OrderDate   time.Time   `json:"order_date"`
	TotalAmount float64     `json:"total_amount"`
	Status      string      `json:"status"`
	Items       []OrderItem `json:"items"`
}

type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

// PopulateDemoData inserts sample data into the database.
func PopulateDemoData(ctx context.Context, opts sop.DatabaseOptions) error {
	trans, err := database.BeginTransaction(ctx, opts, sop.ForWriting)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	comparer := func(a, b string) int {
		return cmp.Compare(a, b)
	}

	// 1. Create Stores
	userStore, err := database.NewBtree[string, UserProfile](ctx, opts, "users", trans, comparer)
	if err != nil {
		trans.Rollback(ctx)
		return fmt.Errorf("failed to open users store: %v", err)
	}

	productStore, err := database.NewBtree[string, Product](ctx, opts, "products", trans, comparer)
	if err != nil {
		trans.Rollback(ctx)
		return fmt.Errorf("failed to open products store: %v", err)
	}

	orderStore, err := database.NewBtree[string, Order](ctx, opts, "orders", trans, comparer)
	if err != nil {
		trans.Rollback(ctx)
		return fmt.Errorf("failed to open orders store: %v", err)
	}

	// 2. Generate Users
	users := make([]UserProfile, 0, 50)
	firstNames := []string{"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth"}
	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}
	countries := []string{"USA", "Canada", "UK", "Germany", "France", "Australia", "Japan"}

	for i := 0; i < 50; i++ {
		fn := firstNames[rand.Intn(len(firstNames))]
		ln := lastNames[rand.Intn(len(lastNames))]
		user := UserProfile{
			ID:        uuid.NewString(),
			FirstName: fn,
			LastName:  ln,
			Email:     fmt.Sprintf("%s.%s@example.com", fn, ln),
			Country:   countries[rand.Intn(len(countries))],
			Age:       18 + rand.Intn(60),
			Gender:    []string{"Male", "Female"}[rand.Intn(2)],
		}
		users = append(users, user)
		if ok, err := userStore.Add(ctx, user.ID, user); err != nil || !ok {
			trans.Rollback(ctx)
			return fmt.Errorf("failed to add user: %v", err)
		}
	}

	// 3. Generate Products
	products := make([]Product, 0, 20)

	productNames := map[string][]string{
		"Electronics": {"Smartphone", "Laptop", "Headphones", "Smart Watch", "Tablet"},
		"Books":       {"Novel", "Science Fiction", "Biography", "Cookbook", "History"},
		"Clothing":    {"T-Shirt", "Jeans", "Jacket", "Sneakers", "Hat"},
		"Home":        {"Coffee Maker", "Blender", "Toaster", "Lamp", "Rug"},
		"Toys":        {"Lego Set", "Action Figure", "Doll", "Puzzle", "Board Game"},
	}

	for cat, names := range productNames {
		for _, name := range names {
			prod := Product{
				ID:          uuid.NewString(),
				Name:        name,
				Description: fmt.Sprintf("High quality %s", name),
				Category:    cat,
				Price:       float64(10 + rand.Intn(990)),
				Stock:       rand.Intn(100),
			}
			products = append(products, prod)
			if ok, err := productStore.Add(ctx, prod.ID, prod); err != nil || !ok {
				trans.Rollback(ctx)
				return fmt.Errorf("failed to add product: %v", err)
			}
		}
	}

	// 4. Generate Orders
	for i := 0; i < 100; i++ {
		user := users[rand.Intn(len(users))]
		numItems := 1 + rand.Intn(5)
		items := make([]OrderItem, 0, numItems)
		total := 0.0

		for j := 0; j < numItems; j++ {
			prod := products[rand.Intn(len(products))]
			qty := 1 + rand.Intn(3)
			item := OrderItem{
				ProductID: prod.ID,
				Quantity:  qty,
				Price:     prod.Price,
			}
			items = append(items, item)
			total += float64(qty) * prod.Price
		}

		order := Order{
			ID:          uuid.NewString(),
			UserID:      user.ID,
			OrderDate:   time.Now().Add(-time.Duration(rand.Intn(30*24)) * time.Hour), // Past 30 days
			TotalAmount: total,
			Status:      []string{"Pending", "Shipped", "Delivered", "Cancelled"}[rand.Intn(4)],
			Items:       items,
		}

		if ok, err := orderStore.Add(ctx, order.ID, order); err != nil || !ok {
			trans.Rollback(ctx)
			return fmt.Errorf("failed to add order: %v", err)
		}
	}

	return trans.Commit(ctx)
}
