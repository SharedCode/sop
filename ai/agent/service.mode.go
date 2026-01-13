package agent

// SessionMode defines the operating mode of the agent session.
type SessionMode int

const (
	// SessionModeInteractive is the default mode where each request is independent.
	SessionModeInteractive SessionMode = iota
	// SessionModePlayback executes a script with state preservation.
	SessionModePlayback
)

func (m SessionMode) String() string {
	switch m {
	case SessionModePlayback:
		return "playback"
	default:
		return "interactive"
	}
}

// GetSessionMode returns the current mode of the session.
func (s *Service) GetSessionMode() SessionMode {
	if s.session.Playback {
		return SessionModePlayback
	}
	return SessionModeInteractive
}
