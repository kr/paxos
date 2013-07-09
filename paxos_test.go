package paxos

import (
	"fmt"
	"testing"
)

var learnerTests = []struct {
	k  int
	ms []*Message
	w  string
}{
	{
		k: 1,
		ms: []*Message{
			{From: 0, Val: "foo", Ar: 1},
		},
		w: "foo",
	},
	{
		k: 3,
		ms: []*Message{
			{From: 0, Val: "foo", Ar: 1},
			{From: 1, Val: "foo", Ar: 1},
		},
		w: "foo",
	},
	{
		k: 3,
		ms: []*Message{
			{From: 0, Val: "foo", Ar: 1},
			{From: 1, Val: "bar", Ar: 2},
			{From: 0, Val: "bar", Ar: 2},
		},
		w: "bar",
	},
	{
		k: 3,
		ms: []*Message{
			{From: 0, Val: "foo", Ar: 1},
			{From: 0, Val: "foo", Ar: 1},
			{From: 1, Val: "bar", Ar: 2},
			{From: 0, Val: "bar", Ar: 2},
		},
		w: "bar",
	},
}

func TestLearner(t *testing.T) {
	for i, tt := range learnerTests {
		ch := make(chan *Message, len(tt.ms))
		for _, m := range tt.ms {
			ch <- m
		}
		g := runLearner(tt.k, ch)
		if g != tt.w {
			t.Errorf("#%d: g = %v want %v", i, g, tt.w)
		}
	}
}

var acceptorTests = []struct {
	ms []*Message
	wm []*Message
}{
	{
		ms: []*Message{
			{Type: prepare, Pr: 1},
		},
		wm: []*Message{
			{Type: promise, Pr: 1, Ar: 0, Val: ""},
		},
	},
	{
		ms: []*Message{
			{Type: prepare, Pr: 2},
			{Type: accept, Pr: 1, Val: "foo"},
		},
		wm: []*Message{
			{Type: promise, Pr: 2, Ar: 0, Val: ""},
		},
	},
	{
		ms: []*Message{
			{Type: accept, Pr: 2, Val: "foo"},
			{Type: prepare, Pr: 1},
		},
		wm: []*Message{
			{Type: accepted, Pr: 0, Ar: 2, Val: "foo"},
		},
	},
	{
		ms: []*Message{
			{Type: prepare, Pr: 2},
			{Type: prepare, Pr: 1},
		},
		wm: []*Message{
			{Type: promise, Pr: 2, Ar: 0, Val: ""},
		},
	},
	{
		ms: []*Message{
			{Type: accept, Pr: 2, Val: "foo"},
			{Type: accept, Pr: 1, Val: "foo"},
		},
		wm: []*Message{
			{Type: accepted, Pr: 0, Ar: 2, Val: "foo"},
		},
	},
	{
		ms: []*Message{
			{Type: accept, Pr: 1, Val: "foo"},
		},
		wm: []*Message{
			{Type: accepted, Ar: 1, Val: "foo"},
		},
	},
	{
		ms: []*Message{
			{Type: accept, Pr: 2, Val: "foo"},
		},
		wm: []*Message{
			{Type: accepted, Ar: 2, Val: "foo"},
		},
	},
	{
		ms: []*Message{
			{Type: accept, Pr: 1, Val: "foo"},
			{Type: prepare, Pr: 2},
		},
		wm: []*Message{
			{Type: accepted, Ar: 1, Val: "foo"},
			{Type: promise, Pr: 2, Ar: 1, Val: "foo"},
		},
	},
	{
		ms: []*Message{
			{Type: accept, Pr: 1, Val: "foo"},
			{Type: accept, Pr: 1, Val: "foo"},
		},
		wm: []*Message{
			{Type: accepted, Pr: 0, Ar: 1, Val: "foo"},
		},
	},
}

func TestAcceptor(t *testing.T) {
	for i, tt := range acceptorTests {
		var gm []*Message
		ch := make(chan *Message, len(tt.ms))
		for _, m := range tt.ms {
			ch <- m
		}
		close(ch)
		f := func(m *Message) {
			gm = append(gm, m)
		}
		runAcceptor(ch, f)
		diff(t, fmt.Sprintf("#%d", i), gm, tt.wm)
	}
}

var coordinatorTests = []struct {
	k  int
	ms []*Message
	wm []*Message
}{
	{
		k: 3,
		ms: []*Message{
			{Type: propose, Val: "foo"},
			{Type: propose, Val: "foo"},
			{From: 0, Type: promise, Pr: 2},
			{From: 1, Type: promise, Pr: 2},
			{From: 2, Type: promise, Pr: 2},
		},
		wm: []*Message{
			{Type: prepare, Pr: 1},
			{Type: prepare, Pr: 4},
		},
	},
	{
		k: 3,
		ms: []*Message{
			{Type: propose, Val: "foo"},
		},
		wm: []*Message{
			{Type: prepare, Pr: 1},
		},
	},
	{
		k: 5,
		ms: []*Message{
			{Type: propose, Val: "foo"},
			{From: 0, Type: promise, Pr: 1},
			{From: 1, Type: promise, Pr: 1},
		},
		wm: []*Message{
			{Type: prepare, Pr: 1},
		},
	},
	{
		k: 5,
		ms: []*Message{
			{Type: propose, Val: "foo"},
			{From: 0, Type: promise, Pr: 1},
			{From: 1, Type: promise, Pr: 1},
			{From: 1, Type: promise, Pr: 1},
		},
		wm: []*Message{
			{Type: prepare, Pr: 1},
		},
	},
	{
		k: 5,
		ms: []*Message{
			{Type: propose, Val: "foo"},
			{From: 0, Type: promise, Pr: 1},
			{From: 1, Type: promise, Pr: 1},
			{From: 2, Type: promise, Pr: 1},
		},
		wm: []*Message{
			{Type: prepare, Pr: 1},
			{Type: accept, Pr: 1, Val: "foo"},
		},
	},
	{
		k: 5,
		ms: []*Message{
			{Type: propose, Val: "foo"},
			{From: 0, Type: promise, Pr: 2},
			{From: 1, Type: promise, Pr: 2},
			{Type: propose, Val: "foo"},
		},
		wm: []*Message{
			{Type: prepare, Pr: 1},
			{Type: prepare, Pr: 6},
		},
	},
	{
		k: 5,
		ms: []*Message{
			{Type: propose, Val: "foo"},
			{Type: propose, Val: "foo"},
			{From: 0, Type: promise, Pr: 6},
			{From: 1, Type: promise, Pr: 6},
			{From: 2, Type: promise, Pr: 6, Ar: 3, Val: "bar"},
		},
		wm: []*Message{
			{Type: prepare, Pr: 1},
			{Type: prepare, Pr: 6},
			{Type: accept, Pr: 6, Val: "bar"},
		},
	},
	{
		k: 5,
		ms: []*Message{
			{Type: propose, Val: "foo"},
			{From: 0, Type: promise, Pr: 1},
			{From: 1, Type: promise, Pr: 1},
			{From: 2, Type: promise, Pr: 1},
			{From: 3, Type: promise, Pr: 1},
		},
		wm: []*Message{
			{Type: prepare, Pr: 1},
			{Type: accept, Pr: 1, Val: "foo"},
		},
	},
	{
		k: 5,
		ms: []*Message{
			{Type: propose, Val: "foo"},
			{From: 0, Type: promise, Pr: 1},
			{From: 1, Type: promise, Pr: 1},
			{From: 2, Type: promise, Pr: 1},
			{Type: propose, Val: "foo"},
			{From: 0, Type: promise, Pr: 6},
			{From: 1, Type: promise, Pr: 6},
			{From: 2, Type: promise, Pr: 6},
		},
		wm: []*Message{
			{Type: prepare, Pr: 1},
			{Type: accept, Pr: 1, Val: "foo"},
			{Type: prepare, Pr: 6},
			{Type: accept, Pr: 6, Val: "foo"},
		},
	},
}

func TestCoordinator(t *testing.T) {
	for i, tt := range coordinatorTests {
		var gm []*Message
		ch := make(chan *Message, len(tt.ms))
		for _, m := range tt.ms {
			ch <- m
		}
		close(ch)
		f := func(m *Message) {
			gm = append(gm, m)
		}
		runCoordinator(tt.k, 1, ch, f)
		diff(t, fmt.Sprintf("#%d", i), gm, tt.wm)
	}
}

var quorumTests = []struct{ k, q int }{
	{3, 2},
	{4, 3},
	{5, 3},
}

func TestQuorum(t *testing.T) {
	for i, tt := range quorumTests {
		g := quorum(tt.k)
		if g != tt.q {
			t.Errorf("#%d: g = %v want %v", i, g, tt.q)
		}
	}
}

func runSet(k int, v string, check func(string)) {
	var ps []*Node
	for _, net := range newTestNet(k) {
		p := Start(net)
		ps = append(ps, p)
		defer p.Stop()
	}
	ps[0].Propose(v)
	for _, p := range ps {
		g := p.Wait()
		check(g)
	}
}

func TestPaxos(t *testing.T) {
	const w = "foo"
	runSet(3, w, func(g string) {
		if w != g {
			t.Errorf("g = %v want %v", g, w)
		}
	})
}

func BenchmarkPaxos(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runSet(3, "foo", func(string) {})
	}
}

func newTestNet(k int) (ns []*testNode) {
	chans := make([]chan *Message, k)
	for i := 0; i < k; i++ {
		chans[i] = make(chan *Message, 1)
		ns = append(ns, &testNode{
			remote: chans,
			local:  chans[i],
			addr:   i,
			len:    k,
		})
	}
	return ns
}

type testNode struct {
	remote []chan *Message
	local  chan *Message
	addr   int
	len    int
}

func (n *testNode) Len() int {
	return n.len
}

func (n *testNode) LocalAddr() int {
	return n.addr
}

func (n *testNode) Recv(m *Message) {
	*m = *<-n.local
}

func (n *testNode) Send(m *Message) {
	for _, c := range n.remote {
		c <- m
	}
}

func diff(t *testing.T, prefix string, g, w []*Message) {
	if len(g) == len(w) {
		for i := range w {
			if *g[i] != *w[i] {
				t.Errorf("%s: g[%d] = %+v want %+v", prefix, i, g[i], w[i])
			}
		}
	} else {
		t.Errorf("%s: g = {", prefix)
		for _, m := range g {
			t.Errorf("\t%+v,", m)
		}
		t.Errorf("}")
		t.Errorf("want {")
		for _, m := range w {
			t.Errorf("\t%+v,", m)
		}
		t.Errorf("}")
	}
}
