package agent

// SessionMode defines the operating mode of the agent session.
type SessionMode int

const (
	// SessionModeInteractive is the default mode where each request is independent.
	SessionModeInteractive SessionMode = iota
	// SessionModeRecording captures steps into a script.
	SessionModeRecording
	// SessionModePlayback executes a script with state preservation.
	SessionModePlayback
)

func (m SessionMode) String() string {
	switch m {
	case SessionModeRecording:
		return "recording"
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
	if s.session.Recording {
		return SessionModeRecording
	}
	return SessionModeInteractive
}
