// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

// optionMethods are the typed forms of options that change what Ruvio does.
var optionMethods = map[string]string{
	"SET IFEQ":                 "SetIfEq",
	"SET IFGT":                 "SetIfGreater",
	"SET IFLT":                 "SetIfLess",
	"SET GET":                  "SetGet",
	"SET options":              "SetArgs",
	"INCRBY MAX":               "IncrByMax",
	"LIMIT SLIDING":            "LimitSliding",
	"LIMIT TOKENS":             "LimitTokens",
	"LIMIT SLIDING WITHRETRY":  "LimitSlidingWithRetry",
	"LIMIT TOKENS WITHRETRY":   "LimitTokensWithRetry",
	"FILLGET WAIT":             "FillGetWait",
	"FILLSET SWR":              "FillSet",
	"IDEM COMPLETE":            "IdemComplete",
	"IDEM ABORT":               "IdemAbort",
	"CHANGES START":            "ChangesStart",
	"RUVIO OWNER":              "RuvioOwner",
	"JSON.INDEX DROP":          "JSONIndexDrop",
	"JSON.INDEX LIST":          "JSONIndexList",
	"JSON.SET NX/XX":           "JSONSetMode",
	"XAUTOCLAIM JUSTID":        "XAutoClaimJustID",
	"XCLAIM JUSTID":            "XClaimJustID",
	"XPENDING extended":        "XPendingExt",
	"OBJECT IDLETIME":          "ObjectIdleTime",
	"OBJECT FREQ":              "ObjectFreq",
	"SLOWLOG LEN":              "SlowLogLen",
	"SLOWLOG RESET":            "SlowLogReset",
	"COMMAND COUNT":            "CommandCount",
	"COMMAND INFO":             "CommandInfo",
	"COMMAND DOCS":             "CommandDocs",
	"HELLO AUTH":               "HelloAuth",
	"EXEC pipelined":           "Tx.Exec",
	"ZRANGE BYSCORE/BYLEX/REV": "ZRangeArgs",
	"ZADD INCR":                "ZAddIncr",
	"GEORADIUS STORE":          "GeoRadiusStore",
	"GEORADIUSBYMEMBER STORE":  "GeoRadiusByMemberStore",
	"PING pubsub":              "PubSub.Ping",
	"LPOP COUNT":               "LPopCount",
	"RPOP COUNT":               "RPopCount",
	"SPOP COUNT":               "SPopCount",
	"BITCOUNT range":           "BitCountRange",
	"LPOS COUNT":               "LPosCount",
}

func hasMethod(spec string) bool {
	typ := reflect.TypeOf(&Client{})
	name := spec
	if i := strings.IndexByte(spec, '.'); i >= 0 {
		switch spec[:i] {
		case "Tx":
			typ = reflect.TypeOf(&Tx{})
		case "PubSub":
			typ = reflect.TypeOf(&PubSub{})
		default:
			return false
		}
		name = spec[i+1:]
	}
	_, ok := typ.MethodByName(name)
	return ok
}

// methodSpecs turns a "go" field of command_methods.json (one name or a list) into names.
func methodSpecs(raw json.RawMessage) []string {
	var one string
	if json.Unmarshal(raw, &one) == nil {
		if one == "" {
			return nil
		}
		return []string{one}
	}
	var many []string
	_ = json.Unmarshal(raw, &many)
	return many
}

// TestCoverage checks that every command of docs/compatibility-matrix.json has a typed method,
// named by the "go" field of clients/testdata/command_methods.json. "Tx.X" and "PubSub.X" live
// on those types; the rest are on *Client.
func TestCoverage(t *testing.T) {
	raw, err := os.ReadFile("../../docs/compatibility-matrix.json")
	if err != nil {
		t.Skip("matrix not found (run inside the repository):", err)
	}
	var m struct {
		Commands []struct {
			Name string `json:"name"`
		} `json:"commands"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile("../testdata/command_methods.json")
	if err != nil {
		t.Fatal(err)
	}
	var table struct {
		Commands map[string]map[string]json.RawMessage `json:"commands"`
	}
	if err := json.Unmarshal(raw, &table); err != nil {
		t.Fatal(err)
	}
	inMatrix := make(map[string]bool, len(m.Commands))
	covered := 0
	for _, c := range m.Commands {
		inMatrix[c.Name] = true
		specs := methodSpecs(table.Commands[c.Name]["go"])
		if len(specs) == 0 {
			t.Errorf("%s: no typed method in command_methods.json", c.Name)
			continue
		}
		ok := true
		for _, spec := range specs {
			if !hasMethod(spec) {
				t.Errorf("%s: method %s does not exist", c.Name, spec)
				ok = false
			}
		}
		if ok {
			covered++
		}
	}
	for name := range table.Commands {
		if !inMatrix[name] {
			t.Errorf("%s: in command_methods.json but not in the matrix", name)
		}
	}
	for opt, spec := range optionMethods {
		if !hasMethod(spec) {
			t.Errorf("option %s: method %s does not exist", opt, spec)
		}
	}
	t.Logf("typed coverage: %d/%d commands, %d option methods", covered, len(m.Commands), len(optionMethods))
	if covered != len(m.Commands) {
		t.Fail()
	}
}
