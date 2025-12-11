package redis

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
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
