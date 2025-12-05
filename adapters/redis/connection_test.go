package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// MockRedisServer is a simple TCP server that mimics enough of Redis to satisfy go-redis
// and return a specific run_id in response to INFO server.
type MockRedisServer struct {
	listener net.Listener
	port     int
	runID    string
	wg       sync.WaitGroup
	quit     chan struct{}
	conns    sync.Map
}

func NewMockRedisServer(port int, runID string) (*MockRedisServer, error) {
	l, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, err
	}
	s := &MockRedisServer{
		listener: l,
		port:     l.Addr().(*net.TCPAddr).Port,
		runID:    runID,
		quit:     make(chan struct{}),
	}
	s.wg.Add(1)
	go s.serve()
	return s, nil
}

func (s *MockRedisServer) serve() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				continue
			}
		}
		s.wg.Add(1)
		s.conns.Store(conn, struct{}{})
		go s.handleConn(conn)
	}
}

func (s *MockRedisServer) handleConn(c net.Conn) {
	defer s.wg.Done()
	defer func() {
		c.Close()
		s.conns.Delete(c)
	}()
	reader := bufio.NewReader(c)

	for {
		// Check for quit
		select {
		case <-s.quit:
			return
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "*") {
			var numArgs int
			fmt.Sscanf(line, "*%d", &numArgs)

			// Read the arguments
			var args []string
			for i := 0; i < numArgs; i++ {
				// Read length
				_, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				// Read value
				val, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				args = append(args, strings.TrimSpace(val))
			}

			if len(args) == 0 {
				continue
			}

			cmd := strings.ToUpper(args[0])
			switch cmd {
			case "INFO":
				content := fmt.Sprintf("# Server\r\nrun_id:%s\r\n", s.runID)
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(content), content)
				c.Write([]byte(resp))
			case "PING":
				c.Write([]byte("+PONG\r\n"))
			case "HELLO":
				// Respond with a Map for RESP3 or just OK?
				// go-redis v9 expects a Map with "server", "version", "proto", "id", "mode", "role", "modules" keys usually.
				// But maybe a simple map is enough.
				// %1\r\n$7\r\nversion\r\n$5\r\n6.0.0\r\n
				c.Write([]byte("%1\r\n$7\r\nversion\r\n$5\r\n6.0.0\r\n"))
			case "COMMAND":
				c.Write([]byte("+OK\r\n"))
			case "CLIENT":
				c.Write([]byte("+OK\r\n"))
			default:
				c.Write([]byte("+OK\r\n"))
			}
		}
	}
}

func (s *MockRedisServer) Stop() {
	close(s.quit)
	s.listener.Close()
	s.conns.Range(func(key, value interface{}) bool {
		key.(net.Conn).Close()
		return true
	})
	s.wg.Wait()
}

func TestRedisRestartDetection(t *testing.T) {
	// Reset globals
	atomic.StoreInt64(&hasRestarted, 0)
	lastSeenRunID.Store("")
	if connection != nil {
		CloseConnection()
	}

	// Start Mock Redis 1
	s1, err := NewMockRedisServer(0, "run_id_11111")
	if err != nil {
		t.Fatalf("Failed to start mock redis 1: %v", err)
	}
	port := s1.port

	// Connect
	opts := Options{
		Address: fmt.Sprintf("localhost:%d", port),
	}
	conn, err := OpenConnection(opts)
	if err != nil {
		s1.Stop()
		t.Fatalf("Failed to open connection: %v", err)
	}

	// Trigger a command to ensure connection and OnConnect is called
	ctx := context.Background()
	conn.Client.Ping(ctx)

	// Give a moment for OnConnect goroutine/callback to finish
	time.Sleep(100 * time.Millisecond)

	// Verify initial state
	if atomic.LoadInt64(&hasRestarted) != 0 {
		t.Errorf("Expected hasRestarted to be 0, got %d", atomic.LoadInt64(&hasRestarted))
	}

	val := lastSeenRunID.Load()
	if val == nil || val.(string) != "run_id_11111" {
		t.Errorf("Expected lastSeenRunID to be run_id_11111, got %v", val)
	}

	// Stop Server 1
	s1.Stop()

	// Start Server 2 on same port with different RunID
	s2, err := NewMockRedisServer(port, "run_id_22222")
	if err != nil {
		t.Fatalf("Failed to start mock redis 2: %v", err)
	}
	defer s2.Stop()

	// Trigger command to force reconnect
	// Loop until success or timeout
	timeout := time.After(2 * time.Second)
	reconnected := false
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for reconnection")
		default:
		}

		err := conn.Client.Ping(ctx).Err()
		if err == nil {
			reconnected = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !reconnected {
		t.Fatal("Failed to reconnect")
	}

	// Give a moment for OnConnect
	time.Sleep(200 * time.Millisecond)

	// Verify restarted detected
	if atomic.LoadInt64(&hasRestarted) != 1 {
		t.Errorf("Expected hasRestarted to be 1, got %d", atomic.LoadInt64(&hasRestarted))
	}

	val = lastSeenRunID.Load()
	if val == nil || val.(string) != "run_id_22222" {
		t.Errorf("Expected lastSeenRunID to be run_id_22222, got %v", val)
	}
}
