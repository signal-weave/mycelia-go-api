package mycelia

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// -------Ack values------------------------------------------------------------

// AckPlcy is how the sender would like to be informed about their message by
// the broker.
// No reply, was it forwarded, etc.
type AckPlcy uint8

const (
	// AckPlcyNoreply prevents sender from receiving an ack.
	AckPlcyNoreply AckPlcy = 0

	// AckPlcyOnsent responds to send with ack when broker delivers to consumer.
	// This often means sending the ack back after the final channel has
	// processed the message object.
	AckPlcyOnsent AckPlcy = 1
)

var ackPolicyName = map[AckPlcy]string{
	AckPlcyNoreply: "NoReply",
	AckPlcyOnsent:  "OnSent",
}

func (ap AckPlcy) String() string {
	return ackPolicyName[ap]
}

// AckType is the response code from the broker.
// Sent, timed out, etc.
type AckType uint8

const (
	AckTypeUnknown AckType = 0 // Undetermined

	// AckTypeSent denotes broker was able to and finished sending message to
	// subscribers.
	AckTypeSent AckType = 1

	// AckTypeTimeout denotes no ack was received before the timeout time, and
	// therefore a response with AckTimeout is generated and returned instead.
	AckTypeTimeout AckType = 10

	AckChannelNotFound      AckType = 20
	AckChannelAlreadyExists AckType = 21
	AckRouteNotFound        AckType = 30
)

var ackTypeName = map[AckType]string{
	AckTypeUnknown:          "Unknown",
	AckTypeSent:             "Sent",
	AckTypeTimeout:          "Timeout",
	AckChannelNotFound:      "Channel not found",
	AckChannelAlreadyExists: "Channel already exists",
	AckRouteNotFound:        "Route not found",
}

func (at AckType) String() string {
	return ackTypeName[at]
}

// Response from the broker with a given ack code and the corresponding
// message's UID.
type Response struct {
	UID string
	Ack AckType
}

// -------Decoding--------------------------------------------------------------

var (
	ErrShortBody      = errors.New("body too short to contain uidLen and ack")
	ErrLengthMismatch = errors.New("body length does not match uidLen+2")
	ErrUIDOverflow    = errors.New("uidLen exceeds body size")
)

// recvAndDecode reads a single message from conn and decodes it according to
// the spec.
// Layout:
//
//	[0..1]   uint16 bodyLen (big-endian)
//	[2]      uint8  uidLen
//	[...]    uid bytes (len = uidLen)
//	[last]   uint8  ack
func recvAndDecode(conn net.Conn) (*Response, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return nil, fmt.Errorf("read body length: %w", err)
	}

	bodyLen := binary.BigEndian.Uint16(hdr[:])
	if bodyLen == 0 {
		return nil, fmt.Errorf("invalid body length: %d", bodyLen)
	}

	body := make([]byte, int(bodyLen))

	// A message could contain an empty uid string so make sure it atleast has
	// the u8 uid len hdr and u8 ack type.
	if len(body) < 2 {
		return nil, ErrShortBody
	}
	uidLen := int(body[0])

	// Validate declared uidLen fits in the body:
	// need 1(uidLen) + uidLen + 1(ack)
	if uidLen < 0 || 1+uidLen+1 > len(body) {
		return nil, ErrUIDOverflow
	}
	// Exact match to the announced bodyLen:
	if 1+uidLen+1 != int(bodyLen) {
		return nil, fmt.Errorf(
			"%w: bodyLen=%d, expected=%d",
			ErrLengthMismatch, bodyLen, 1+uidLen+1,
		)
	}

	uid := string(body[1 : 1+uidLen])
	ack := body[1+uidLen]

	return &Response{
		UID: uid,
		Ack: AckType(ack),
	}, nil
}
