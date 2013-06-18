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
	nop = iota
	prepare
	promise
	accept
	accepted
	propose
)

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
	// All fields except From will be set by the caller;
	// it is the network's responsibility to set From properly.
	Send(*Message)

	// Recv receives a message from any node on the network.
	Recv(*Message)

	// Len returns the number of nodes on the network.
	Len() int

	// LocalAddr returns the local network address.
	LocalAddr() int
}

// Node represents a paxos participant.
type Node struct {
	t    Network
	prop chan string
}

// NewNode returns a participant for paxos on network t.
func NewNode(t Network) *Node {
	return &Node{t, make(chan string, 1)}
}

// Run does the work of a paxos participant in all three roles
// (coordinator, acceptor, and learner).
// It returns the value learned.
func (n *Node) Run() string {
	cch := make(chan *Message)
	ach := make(chan *Message)
	lch := make(chan *Message)
	stop := make(chan int)
	defer close(cch)
	defer close(ach)
	defer func() { stop <- 1 }()
	k := n.t.Len()
	pr := int64(n.t.LocalAddr())
	if pr == 0 {
		pr += int64(k)
	}
	go n.mux(stop, cch, ach, lch)
	go runCoordinator(k, pr, cch, n.t.Send)
	go runAcceptor(ach, n.t.Send)
	return runLearner(k, lch)
}

// Propose tells the coordinator to start a new round.
func (n *Node) Propose(v string) {
	select {
	case n.prop <- v:
	default:
	}
}

func (n *Node) mux(stop chan int, chs ...chan *Message) {
	for {
		m := new(Message)
		select {
		case m.Val = <-n.prop:
			m.Type = propose
		default:
			n.t.Recv(m)
		}
		c := chs[role(m.Type)]
		select {
		case c <- m:
		case <-stop:
			return
		}
	}
}

func role(cmd int) int {
	switch cmd {
	case propose, promise:
		return 0
	case prepare, accept:
		return 1
	case accepted:
		return 2
	}
	return -1
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
	for {
		m := <-recv
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
}

func quorum(k int) int {
	return k/2 + 1
}
