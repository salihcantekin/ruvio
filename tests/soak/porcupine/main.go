// Command porcupine checks that the history of every LEASE and SEMAPHORE key written by the soak
// tool is linearizable (see ../ruvio_soak/primitives.py for the format).
//
//	go run . timeout_seconds history.jsonl [more segments in time order...]
//
// It prints one JSON object: keys checked, keys that are not linearizable (`illegal`) and keys
// the search did not finish for (`unknown`). Exit status 0 unless a key is illegal.
//
// The model of one key: a set of held tokens and the largest token granted so far. A grant that
// answered a token needs fewer holders than `max` and a token above every earlier one; a refused
// grant needs `max` holders; a release answering 1 needs the token held, answering 0 needs it
// not held. An operation whose outcome is unknown may or may not have happened. A holder that
// never released expires, which the model allows to happen at any time after its time to live;
// that is looser than the server, so it can miss a violation but never invent one.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"
)

type entry struct {
	P    string          `json:"p"`
	I    int             `json:"i"`
	Type string          `json:"type"`
	F    string          `json:"f"`
	K    string          `json:"k"`
	V    json.RawMessage `json:"v"`
	T    int64           `json:"t"`
}

type input struct {
	Kind  string // grant, release, expire
	Max   int
	Token int64 // release and expire; 0 for a phantom (a grant of unknown outcome)
}

type output struct {
	Unknown bool
	Token   int64 // grant: 0 means refused
	Ret     int   // release: 1 or 0
}

type op struct {
	start, end int64
	client     string
	in         input
	out        output
	typ        string
	ttlNs      int64
}

// state is "last|token,token|phantoms".
type state struct {
	last     int64
	held     []int64
	phantoms int
}

func decode(s interface{}) state {
	parts := strings.Split(s.(string), "|")
	st := state{}
	st.last, _ = strconv.ParseInt(parts[0], 10, 64)
	if parts[1] != "" {
		for _, t := range strings.Split(parts[1], ",") {
			v, _ := strconv.ParseInt(t, 10, 64)
			st.held = append(st.held, v)
		}
	}
	st.phantoms, _ = strconv.Atoi(parts[2])
	return st
}

func (st state) encode() string {
	sort.Slice(st.held, func(i, j int) bool { return st.held[i] < st.held[j] })
	strs := make([]string, len(st.held))
	for i, v := range st.held {
		strs[i] = strconv.FormatInt(v, 10)
	}
	return fmt.Sprintf("%d|%s|%d", st.last, strings.Join(strs, ","), st.phantoms)
}

func (st state) index(token int64) int {
	for i, v := range st.held {
		if v == token {
			return i
		}
	}
	return -1
}

func (st state) without(token int64) state {
	next := state{last: st.last, phantoms: st.phantoms}
	for _, v := range st.held {
		if v != token {
			next.held = append(next.held, v)
		}
	}
	return next
}

func (st state) with(token int64) state {
	next := state{last: st.last, phantoms: st.phantoms, held: append(append([]int64{}, st.held...), token)}
	if token > next.last {
		next.last = token
	}
	return next
}

func step(s, i, o interface{}) []interface{} {
	st, in, out := decode(s), i.(input), o.(output)
	holders := len(st.held) + st.phantoms
	switch in.Kind {
	case "grant":
		if out.Unknown {
			// Not applied, or applied with a token nobody saw.
			next := st
			next.phantoms++
			return []interface{}{st.encode(), next.encode()}
		}
		if out.Token == 0 {
			if holders >= in.Max {
				return []interface{}{st.encode()}
			}
			return nil
		}
		if holders < in.Max && out.Token > st.last {
			return []interface{}{st.with(out.Token).encode()}
		}
		return nil
	case "release":
		held := st.index(in.Token) >= 0
		if out.Unknown {
			if held {
				return []interface{}{st.encode(), st.without(in.Token).encode()}
			}
			return []interface{}{st.encode()}
		}
		if out.Ret == 1 && held {
			return []interface{}{st.without(in.Token).encode()}
		}
		if out.Ret == 0 && !held {
			return []interface{}{st.encode()}
		}
		return nil
	case "expire":
		if in.Token == 0 {
			if st.phantoms > 0 {
				next := st
				next.phantoms--
				return []interface{}{st.encode(), next.encode()}
			}
			return []interface{}{st.encode()}
		}
		if st.index(in.Token) >= 0 {
			return []interface{}{st.encode(), st.without(in.Token).encode()}
		}
		return []interface{}{st.encode()}
	}
	return nil
}

var nondeterministic = porcupine.NondeterministicModel{
	Init:  func() []interface{} { return []interface{}{"0||0"} },
	Step:  step,
	Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
}

var model = nondeterministic.ToModel()

func asInt(raw json.RawMessage) (int64, bool) {
	var v *int64
	if json.Unmarshal(raw, &v) != nil || v == nil {
		return 0, false
	}
	return *v, true
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: porcupine timeout_seconds history.jsonl...")
		os.Exit(2)
	}
	timeout := 120 * time.Second
	if s, err := strconv.Atoi(os.Args[1]); err == nil {
		timeout = time.Duration(s) * time.Second
	}

	open := map[string]*op{}
	keyOf := map[string]string{}
	perKey := map[string][]*op{}
	for _, name := range os.Args[2:] {
		file, err := os.Open(name)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		read(file, open, keyOf, perKey)
		file.Close()
	}
	check(perKey, timeout)
}

func read(file *os.File, open map[string]*op, keyOf map[string]string, perKey map[string][]*op) {
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 1<<26)
	for scanner.Scan() {
		var e entry
		if json.Unmarshal(scanner.Bytes(), &e) != nil {
			continue
		}
		if e.F != "lease" && e.F != "semaphore" && e.F != "release" {
			continue
		}
		id := e.P + "/" + strconv.Itoa(e.I)
		if e.Type == "invoke" {
			o := &op{start: e.T, end: math.MaxInt64, client: e.P, typ: "info"}
			if e.F == "release" {
				token, _ := asInt(e.V)
				o.in = input{Kind: "release", Token: token}
			} else {
				var args []int64
				_ = json.Unmarshal(e.V, &args)
				o.in = input{Kind: "grant", Max: int(args[0])}
				o.ttlNs = args[1] * 1_000_000
			}
			o.out = output{Unknown: true}
			open[id], keyOf[id] = o, e.K
			perKey[e.K] = append(perKey[e.K], o)
			continue
		}
		o := open[id]
		if o == nil {
			continue
		}
		o.typ, o.end = e.Type, e.T
		if e.Type == "ok" {
			v, present := asInt(e.V)
			if o.in.Kind == "grant" {
				o.out = output{Token: v}
				_ = present
			} else {
				o.out = output{Ret: int(v)}
			}
		}
	}

}

func check(perKey map[string][]*op, timeout time.Duration) {
	result := map[string]interface{}{"illegal": []string{}, "unknown": []string{}}
	illegal, unknown := []string{}, []string{}
	keys := make([]string, 0, len(perKey))
	for k := range perKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		var history []porcupine.Operation
		client := map[string]int{}
		id := func(name string) int {
			if _, ok := client[name]; !ok {
				client[name] = len(client)
			}
			return client[name]
		}
		for _, o := range perKey[key] {
			if o.typ == "fail" {
				continue // certainly not applied
			}
			history = append(history, porcupine.Operation{ClientId: id(o.client), Input: o.in, Call: o.start, Output: o.out, Return: o.end})
			if o.in.Kind == "grant" {
				token := o.out.Token
				if o.typ != "ok" {
					token = 0
				}
				if o.typ != "ok" || token != 0 {
					history = append(history, porcupine.Operation{
						ClientId: id("expiry"), Input: input{Kind: "expire", Token: token}, Call: o.start + o.ttlNs,
						Output: output{}, Return: math.MaxInt64,
					})
				}
			}
		}
		switch porcupine.CheckOperationsTimeout(model, history, timeout) {
		case porcupine.Illegal:
			illegal = append(illegal, key)
		case porcupine.Unknown:
			unknown = append(unknown, key)
		}
	}
	result["keys"], result["illegal"], result["unknown"] = len(keys), illegal, unknown
	out, _ := json.Marshal(result)
	fmt.Println(string(out))
	if len(illegal) > 0 {
		os.Exit(1)
	}
}
