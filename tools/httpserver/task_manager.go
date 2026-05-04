package main

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Task struct {
	TaskID    string    `json:"task_id"`
	TaskType  string    `json:"task_type"` // e.g., "KnowledgePreload"
	Status    string    `json:"status"`    // "pending", "in_progress", "completed", "error"
	Progress  int       `json:"progress"`
	Total     int       `json:"total"`
	Message   string    `json:"message,omitempty"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

var (
	tasksMutex sync.RWMutex
	tasksStore = make(map[string]*Task)
)

func RegisterTask(taskType string, total int) *Task {
	t := &Task{
		TaskID:    uuid.New().String(),
		TaskType:  taskType,
		Status:    "pending",
		Progress:  0,
		Total:     total,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	tasksMutex.Lock()
	defer tasksMutex.Unlock()
	tasksStore[t.TaskID] = t
	return t
}

func UpdateTask(taskID string, status string, progress int, total int, msg string, errStr string) {
	tasksMutex.Lock()
	defer tasksMutex.Unlock()
	if t, exists := tasksStore[taskID]; exists {
		if status != "" {
			t.Status = status
		}
		t.Progress = progress
		if total > 0 {
			t.Total = total
		}
		t.Message = msg
		t.Error = errStr
		t.UpdatedAt = time.Now()
	}
}

func GetTask(taskID string) *Task {
	tasksMutex.RLock()
	defer tasksMutex.RUnlock()
	if t, exists := tasksStore[taskID]; exists {
		tCopy := *t
		return &tCopy
	}
	return nil
}

func handleGetTaskStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	taskID := r.URL.Query().Get("task_id")
	if taskID == "" {
		http.Error(w, "task_id is required", http.StatusBadRequest)
		return
	}

	task := GetTask(taskID)
	if task == nil {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(task)
}
