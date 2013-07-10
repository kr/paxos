// Package paxos implements the basic paxos consensus protocol.
// The interface of this package is experimental and may change.
package paxos

// Paxos participants use Message for communication amongst themselves.
// Messages are exchanged over a Network.
type Message struct {
	From int // network address of the sender
	Type int
	Pr   int64
	Ar   int64
	Val  string
}

const (
	none = iota - 1
	coordinator
	acceptor
	learner
)

const (
	nop = iota
	prepare
	promise
	accept
	accepted
	propose
)

var role = [...]int{
	nop:      none,
	propose:  coordinator,
	promise:  coordinator,
	prepare:  acceptor,
	accept:   acceptor,
	accepted: learner,
}

// A Network broadcasts and receives messages.
// Paxos makes the following assumptions:
//   - messages can be delayed arbitrarily or lost
//   - messages that do arrive will be unaltered
//   - messages can arrive out of order
// Each node on the network has a unique address.
// On a network of k nodes, addresses are the integers in [0,k).
type Network interface {
	// Send sends a message to all nodes on the network.
	// It may be called from multiple goroutines concurrently.
	// If Send encounters an error, it should make a reasonable
	// effort to deliver the message, then return. (Paxos
	// doesn't distinguish between detected and undetected
	// failures in delivery.)
	Send(*Message)

	// Recv receives a message from any node on the network.
	// It should block until one complete message has been
	// successfully received.
	Recv(*Message)

	// Len returns the number of nodes on the network.
	Len() int

	// LocalAddr returns the local network address.
	LocalAddr() int
}

// Node represents a paxos participant.
type Node struct {
	prop    chan string
	stop    chan int
	learned chan int
	v       string
}

// Start runs paxos on network t.
func Start(t Network) *Node {
	n := &Node{
		prop:    make(chan string, 1),
		stop:    make(chan int, 1),
		learned: make(chan int),
	}
	go n.run(t)
	return n
}

// run does the work of a paxos participant in all three roles
// (coordinator, acceptor, and learner).
func (n *Node) run(t Network) {
	mch := []chan *Message{
		coordinator: make(chan *Message),
		acceptor:    make(chan *Message),
		learner:     make(chan *Message),
	}
	defer func() {
		for _, c := range mch {
			close(c)
		}
	}()
	addr := t.LocalAddr()
	send := func(m *Message) {
		m.From = addr
		go t.Send(m)
	}
	k := t.Len()
	pr := int64(addr)
	if pr == 0 {
		pr += int64(k)
	}
	go runCoordinator(k, pr, mch[coordinator], send)
	go runAcceptor(mch[acceptor], send)
	go func() {
		n.v = runLearner(k, mch[learner])
		close(n.learned)
		for _ = range mch[learner] {
		}
	}()
	for {
		m := new(Message)
		select {
		case m.Val = <-n.prop:
			m.Type = propose
		default:
			t.Recv(m)
		}
		select {
		case mch[role[m.Type]] <- m:
		case <-n.stop:
			return
		}
	}
}

// Propose starts a new round proposing v.
// If n has been stopped, Propose has no effect.
func (n *Node) Propose(v string) {
	select {
	case n.prop <- v:
	default:
	}
}

// Wait returns the value learned.
// If n is stopped before it learns a value,
// Wait returns the empty string.
func (n *Node) Wait() string {
	<-n.learned
	return n.v
}

// Stop ceases participation in Paxos.
func (n *Node) Stop() {
	select {
	case n.stop <- 1:
	default:
	}
}

func runCoordinator(k int, pr int64, recv chan *Message, send func(*Message)) {
	var (
		v          string
		ar         int64
		av         string
		sentAccept bool
		npromised  int
		promised   = make([]bool, k)
		q          = quorum(k)
	)
	pr -= int64(k)
	for m := range recv {
		switch m.Type {
		case propose:
			v = m.Val
			pr += int64(k)
			send(&Message{Type: prepare, Pr: pr})
			sentAccept = false
		case promise:
			if m.Pr != pr {
				break
			}
			if m.Ar > ar {
				ar = m.Ar
				av = m.Val
			}
			if !promised[m.From] {
				promised[m.From] = true
				npromised++
			}
			if npromised >= q && !sentAccept {
				use := v
				if ar > 0 {
					use = av
				}
				send(&Message{Type: accept, Pr: pr, Val: use})
				sentAccept = true
			}
		}
	}
}

func runAcceptor(recv chan *Message, send func(*Message)) {
	var pr, ar int64
	var v string
	for m := range recv {
		switch m.Type {
		case prepare:
			if m.Pr > pr {
				pr = m.Pr
				send(&Message{Type: promise, Pr: pr, Ar: ar, Val: v})
			}
		case accept:
			if m.Pr >= pr && m.Pr != ar {
				pr = m.Pr
				ar = m.Pr
				v = m.Val
				send(&Message{Type: accepted, Ar: ar, Val: v})
			}
		}
	}
}

func runLearner(k int, recv chan *Message) string {
	var round int64 = 1
	votes := make(map[string]int)
	voted := make([]bool, k)
	q := quorum(k)
	for m := range recv {
		if m.Ar > round {
			round = m.Ar
			votes = make(map[string]int)
			voted = make([]bool, k)
		}
		if m.Ar == round {
			if !voted[m.From] {
				voted[m.From] = true
				votes[m.Val]++
			}
			if votes[m.Val] >= q {
				return m.Val
			}
		}
	}
	return ""
}

func quorum(k int) int {
	return k/2 + 1
}
