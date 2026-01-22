package metrics

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// Renderer renders metrics updates to a TTY.
// This implementation uses Bubble Tea so it can react to window resizes cleanly.
type Renderer struct {
	out io.Writer
	s   *Collector

	p        *tea.Program
	done     chan struct{}
	runErrMu sync.Mutex
	runErr   error

	stopOnce sync.Once
}

func NewTTYRenderer(s *Collector, out io.Writer) *Renderer {
	r := &Renderer{
		out:  out,
		s:    s,
		done: make(chan struct{}),
	}

	m := newMetricsModel(s)
	// Use stdin so the program stays alive; ignore key messages in the model.
	r.p = tea.NewProgram(
		m,
		tea.WithOutput(out),
		tea.WithInput(os.Stdin),
	)

	go func() {
		defer close(r.done)
		_, err := r.p.Run()
		r.runErrMu.Lock()
		r.runErr = err
		r.runErrMu.Unlock()
	}()

	return r
}

func (r *Renderer) Close() error {
	r.stopOnce.Do(func() {
		// Prefer a model-level stop so we can quit cleanly.
		r.p.Send(stopMsg{})
	})

	<-r.done

	r.runErrMu.Lock()
	defer r.runErrMu.Unlock()
	return r.runErr
}

type tickMsg time.Time
type stopMsg struct{}

type metricsModel struct {
	s *Collector

	width  int
	height int
}

func newMetricsModel(s *Collector) metricsModel {
	return metricsModel{s: s}
}

func (m metricsModel) Init() tea.Cmd {
	return tickCmd()
}

func tickCmd() tea.Cmd {
	return tea.Tick(100*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m metricsModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case stopMsg:
		return m, tea.Quit

	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil

	case tickMsg:
		// Re-render on a timer; Bubble Tea will diff/paint efficiently.
		return m, tickCmd()

	case tea.KeyMsg:
		// Ignore input; this renderer shouldn't be interactive.
		return m, nil

	default:
		return m, nil
	}
}

func (m metricsModel) View() string {
	// Render the tree into a buffer, then adapt it to the current viewport.
	// Bubble Tea will handle repainting and resizes.
	b := &bytes.Buffer{}
	if m.s == nil {
		b.WriteString("(no metrics)\n")
	} else {
		_ = RenderTree(b, m.s, "")
	}

	return fitToViewport(b.String(), m.width, m.height)
}

func fitToViewport(s string, width, height int) string {
	// Normalize newlines for stable line handling.
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.TrimRight(s, "\n")

	lines := strings.Split(s, "\n")

	// Width: truncate each line (Bubble Tea's renderer will handle clearing/padding).
	if width > 0 {
		for i := range lines {
			lines[i] = truncateRunes(lines[i], width)
		}
	}

	// Height: limit number of lines if we know it.
	if height > 0 && len(lines) > height {
		remaining := len(lines) - (height - 1)
		lines = append(lines[:max(0, height-1)], fmt.Sprintf("... (%d more)", remaining))
		if width > 0 {
			lines[len(lines)-1] = truncateRunes(lines[len(lines)-1], width)
		}
	}

	return strings.Join(lines, "\n") + "\n"
}

func truncateRunes(s string, width int) string {
	if width <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) <= width {
		return s
	}
	if width <= 1 {
		return string(r[:width])
	}
	// Use a single ellipsis rune if we have room.
	return string(r[:width-1]) + "…"
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// RenderTree renders the state of the collector to buf.
func RenderTree(buf *bytes.Buffer, x *Collector, indent string) error {
	buf.WriteString(indent)
	fmt.Fprintf(buf, "[%v] %s ", x.Duration().Round(time.Millisecond), x.name)
	for i, k := range x.List() {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(buf, "%s=%v", k, x.GetCounter(k))
	}
	buf.WriteString("\n")
	for _, k := range x.ListChildren() {
		x2 := x.GetChild(k)
		if x2 == nil {
			continue
		}
		RenderTree(buf, x2, indent+"  ")
	}
	return nil
}
