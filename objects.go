package mycelia

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/google/uuid"
	"github.com/signal-weave/rhizome"
)

const (
	ObjMessage     uint8 = 1
	ObjTransformer uint8 = 2
	ObjSubscriber  uint8 = 3
	ObjChannel     uint8 = 4

	ObjGlobals uint8 = 20

	ObjAction uint8 = 50
)

const (
	CmdUnknown uint8 = 0

	CmdSend   uint8 = 1
	CmdAdd    uint8 = 2
	CmdRemove uint8 = 3

	CmdUpdate uint8 = 20

	CmdSigterm uint8 = 50
)

const (
	ApiProtocolVer uint8  = 1
	maxU16Len      uint32 = 65535
)

// DeadLetter is used for subscribing to dead letter channels.
const DeadLetter = "deadLetter"

// -------Public message types--------------------------------------------------

type Command interface {
	CmdValid() bool
	EffectiveCmd() uint8
}

// Message sends a payload over a route.
type Message struct {
	AckPolicy AckPlcy
	Route     string
	Payload   []byte
	Encoding  PayloadEncoding
	// Optional: override, defaults to CmdSend if zero.
	CmdType uint8
}

func (m Message) CmdValid() bool {
	c := m.EffectiveCmd()
	return c == CmdSend
}

func (m Message) EffectiveCmd() uint8 {
	if m.CmdType != CmdUnknown {
		return m.CmdType
	}
	return CmdSend
}

// Transformer registers/unregisters a transformer at a channel.
type Transformer struct {
	AckPolicy AckPlcy
	Route     string
	Channel   string
	Address   string
	// Optional: override, defaults to CmdAdd if zero.
	CmdType uint8
}

func (t Transformer) CmdValid() bool {
	c := t.EffectiveCmd()
	return c == CmdAdd || c == CmdRemove
}
func (t Transformer) EffectiveCmd() uint8 {
	if t.CmdType != CmdUnknown {
		return t.CmdType
	}
	return CmdAdd
}

// Subscriber registers/unregisters a subscriber at a channel.
type Subscriber struct {
	AckPolicy AckPlcy
	Route     string
	Channel   string
	Address   string
	// Optional: override, defaults to CmdAdd if zero.
	CmdType uint8
}

func (s Subscriber) CmdValid() bool {
	c := s.EffectiveCmd()
	return c == CmdAdd || c == CmdRemove
}
func (s Subscriber) EffectiveCmd() uint8 {
	if s.CmdType != CmdUnknown {
		return s.CmdType
	}
	return CmdAdd
}

type GlobalValues struct {
	SecurityToken    string
	Address          string // '' = ignore
	Port             int    // 0..65535 valid; others ignored
	Verbosity        int    // 0..3 valid; others ignored
	PrintTree        *bool  // nil = ignore
	TransformTimeout string // '' = ignore (e.g. "500ms")
	Consolidate      *bool  // nil = ignore
}

// Globals updates broker globals.
type Globals struct {
	AckPolicy AckPlcy
	Values    GlobalValues
	// Optional: override, defaults to CmdUpdate if zero.
	CmdType uint8
}

func (g Globals) CmdValid() bool {
	c := g.EffectiveCmd()
	return c == CmdUpdate
}

func (g Globals) EffectiveCmd() uint8 {
	if g.CmdType != CmdUnknown {
		return g.CmdType
	}
	return CmdUpdate
}

type Channel struct {
	AckPolicy AckPlcy
	Route     string
	Name      string
	// Optional: override, defaults to SelectionStratPubsub if zero.
	SelectionStrategy SelectionStrat
	// Optional: override, defaults to CmdAdd if zero.
	CmdType uint8
}

func (c Channel) CmdValid() bool {
	cmd := c.EffectiveCmd()
	return cmd == CmdAdd || cmd == CmdRemove
}

func (c Channel) EffectiveCmd() uint8 {
	if c.CmdType != CmdUnknown {
		return c.CmdType
	}
	return CmdAdd
}

// Action invokes application level commands of the broker.
type Action struct {
	AckPolicy AckPlcy
	// Optional: override, defaults to CmdSigterm if zero.
	CmdType uint8
}

func (a Action) CmdValid() bool {
	c := a.EffectiveCmd()
	return c == CmdSigterm
}
func (a Action) EffectiveCmd() uint8 {
	if a.CmdType != CmdUnknown {
		return a.CmdType
	}
	return CmdSigterm
}

// -------Frame builder---------------------------------------------------------

type frame struct {
	objType    uint8
	cmdType    uint8
	ackPlcy    uint8
	arg1, arg2 string
	arg3, arg4 string

	payloadEncoding PayloadEncoding
	payloadBytes    []byte
}

func encodeMessage(msg Message) (*rhizome.Object, error) {
	obj := rhizome.NewObject(
		ObjMessage, msg.EffectiveCmd(), msg.AckPolicy.Uint8(),
		uuid.NewString(),
		msg.Route, "", "", "", // args
		rhizome.PayloadEncoding(msg.Encoding),
		msg.Payload,
	)

	return obj, nil
}

func encodeTransformer(tfr Transformer) (*rhizome.Object, error) {
	obj := rhizome.NewObject(
		ObjTransformer, tfr.EffectiveCmd(), tfr.AckPolicy.Uint8(),
		uuid.NewString(),
		tfr.Route, tfr.Channel, tfr.Address, "", // args
		rhizome.EncodingNA,
		[]byte{},
	)

	return obj, nil
}

func encodeSubscriber(sub Subscriber) (*rhizome.Object, error) {
	obj := rhizome.NewObject(
		ObjSubscriber, sub.EffectiveCmd(), sub.AckPolicy.Uint8(),
		uuid.NewString(),
		sub.Route, sub.Channel, sub.Address, "", // args
		rhizome.EncodingNA,
		[]byte{},
	)

	return obj, nil
}

func encodeGlobals(glb Globals) (*rhizome.Object, error) {
	data := make(map[string]any)
	if glb.Values.Address != "" {
		data["address"] = glb.Values.Address
	}
	if glb.Values.Port > 0 && glb.Values.Port < 65536 {
		data["port"] = glb.Values.Port
	}
	if glb.Values.Verbosity >= 0 && glb.Values.Verbosity < 4 {
		data["verbosity"] = glb.Values.Verbosity
	}
	if glb.Values.PrintTree != nil {
		data["print_tree"] = *glb.Values.PrintTree
	}
	if glb.Values.TransformTimeout != "" {
		data["transform_timeout"] = glb.Values.TransformTimeout
	}
	if glb.Values.Consolidate != nil {
		data["consolidate"] = *glb.Values.Consolidate
	}
	if glb.Values.SecurityToken != "" {
		return nil, errors.New("globals: security token required")
	}
	if len(data) == 0 {
		return nil, errors.New("globals: no valid fields to encode")
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("globals: marshal: %w", err)
	}

	obj := rhizome.NewObject(
		ObjGlobals, glb.EffectiveCmd(), glb.AckPolicy.Uint8(),
		uuid.NewString(),
		"", "", "", "", // args
		rhizome.EncodingJson,
		payload,
	)

	return obj, nil
}

func encodeChannel(ch Channel) (*rhizome.Object, error) {
	obj := rhizome.NewObject(
		ObjChannel, ch.EffectiveCmd(), ch.AckPolicy.Uint8(),
		uuid.NewString(),
		ch.Route, ch.Name, ch.SelectionStrategy.String(), "", // args
		rhizome.EncodingNA,
		[]byte{},
	)

	return obj, nil
}

func encodeAction(act Action) (*rhizome.Object, error) {
	obj := rhizome.NewObject(
		ObjAction, act.EffectiveCmd(), act.AckPolicy.Uint8(),
		uuid.NewString(),
		"", "", "", "", // args
		rhizome.EncodingNA,
		[]byte{},
	)

	return obj, nil
}

func encode(cmd Command) ([]byte, error) {
	var obj *rhizome.Object
	var err error

	switch v := cmd.(type) {
	case Message:
		obj, err = encodeMessage(v)
		if err != nil {
			return nil, err
		}
	case Transformer:
		obj, err = encodeTransformer(v)
		if err != nil {
			return nil, err
		}
	case Subscriber:
		obj, err = encodeSubscriber(v)
		if err != nil {
			return nil, err
		}
	case Globals:
		obj, err = encodeGlobals(v)
		if err != nil {
			return nil, err
		}
	case Channel:
		obj, err = encodeChannel(v)
		if err != nil {
			return nil, err
		}
	case Action:
		obj, err = encodeAction(v)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported object type %T", cmd)
	}

	b, err := rhizome.EncodeFrame(obj)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// Send connects to address:port and transmits the encoded frame.
// Returns *Response or error.
func Send(cmd Command, address string, port int) (*Response, error) {
	frame, err := encode(cmd)
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial("tcp", net.JoinHostPort(address, strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	_, err = conn.Write(frame)
	if err != nil {
		return nil, err
	}

	response, err := recvAndDecode(conn)
	if err != nil {
		return nil, err
	}

	return response, nil
}
