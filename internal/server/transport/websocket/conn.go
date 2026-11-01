package websocket

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	TextMessage       MessageType = 1
	BinaryMessage     MessageType = 2
	closeMessage      MessageType = 8
	pingMessage       MessageType = 9
	pongMessage       MessageType = 10
	closeWriteTimeout             = 100 * time.Millisecond
)

// MessageType identifies the two JSON-bearing WebSocket data message kinds.
// Control types are handled by Conn and are not returned to callers.
type MessageType uint8

var (
	ErrProtocol         = errors.New("websocket: protocol error")
	ErrMessageTooLarge  = errors.New("websocket: message exceeds configured limit")
	ErrSlowConsumer     = errors.New("websocket: slow consumer")
	ErrConnectionClosed = errors.New("websocket: connection closed")
)

type closeError struct {
	code   uint16
	reason string
}

func (e *closeError) Error() string {
	if e.reason == "" {
		return fmt.Sprintf("websocket: peer closed with code %d", e.code)
	}
	return fmt.Sprintf("websocket: peer closed with code %d: %s", e.code, e.reason)
}

type frame struct {
	opcode  byte
	fin     bool
	payload []byte
}

// Conn is one authenticated WebSocket connection. ReadMessage may be called
// by one goroutine and WriteMessage by another; writes are serialized. It is
// intentionally independent of RPC and can also carry watch.Frame bytes.
type Conn struct {
	netConn net.Conn
	reader  *bufio.Reader
	writer  *bufio.ReadWriter
	cfg     Config

	writeMu sync.Mutex
	closeMu sync.Mutex
	closed  bool
}

func newConn(conn net.Conn, rw *bufio.ReadWriter, cfg Config) *Conn {
	return &Conn{netConn: conn, reader: rw.Reader, writer: rw, cfg: cfg}
}

// ReadMessage reads one complete text or binary message. Client frames must
// be masked as required by RFC 6455. Fragmentation and interleaved control
// frames are handled here, before any bytes reach the JSON-RPC dispatcher.
func (c *Conn) ReadMessage(ctx context.Context) (MessageType, []byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, func() { _ = c.netConn.SetReadDeadline(time.Now()) })
	defer stop()

	var (
		messageType MessageType
		message     []byte
		fragmented  bool
	)
	for {
		if err := c.setReadDeadline(ctx); err != nil {
			return 0, nil, err
		}
		f, err := c.readFrame()
		if err != nil {
			return 0, nil, err
		}
		switch f.opcode {
		case byte(pingMessage), byte(pongMessage):
			// Control frames may be interleaved with fragmented data. Handle
			// them here so the fragmentation state survives the control frame.
			if f.opcode == byte(pingMessage) {
				if err := c.writePong(ctx, f.payload); err != nil {
					return 0, nil, err
				}
			}
			continue
		case byte(closeMessage):
			return 0, nil, &closeError{code: closeCode(f.payload), reason: closeReason(f.payload)}
		case byte(TextMessage), byte(BinaryMessage):
			if fragmented {
				return 0, nil, ErrProtocol
			}
			messageType = MessageType(f.opcode)
			message = append(message[:0], f.payload...)
			fragmented = !f.fin
			if !fragmented {
				if messageType == TextMessage && !utf8.Valid(message) {
					return 0, nil, ErrProtocol
				}
				return messageType, message, nil
			}
		case 0:
			if !fragmented {
				return 0, nil, ErrProtocol
			}
			if int64(len(message))+int64(len(f.payload)) > c.cfg.MaxMessageBytes {
				return 0, nil, ErrMessageTooLarge
			}
			message = append(message, f.payload...)
			if f.fin {
				if messageType == TextMessage && !utf8.Valid(message) {
					return 0, nil, ErrProtocol
				}
				return messageType, message, nil
			}
		default:
			return 0, nil, ErrProtocol
		}
		if int64(len(message)) > c.cfg.MaxMessageBytes {
			return 0, nil, ErrMessageTooLarge
		}
	}
}

func (c *Conn) setReadDeadline(ctx context.Context) error {
	deadline := time.Now().Add(c.cfg.ReadTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	return c.netConn.SetReadDeadline(deadline)
}

func (c *Conn) readFrame() (frame, error) {
	var header [2]byte
	if _, err := io.ReadFull(c.reader, header[:]); err != nil {
		if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
			return frame{}, ErrConnectionClosed
		}
		return frame{}, err
	}
	if header[0]&0x70 != 0 {
		return frame{}, ErrProtocol
	}
	fin := header[0]&0x80 != 0
	opcode := header[0] & 0x0f
	masked := header[1]&0x80 != 0
	if !masked {
		return frame{}, ErrProtocol
	}
	length, err := c.readLength(header[1] & 0x7f)
	if err != nil {
		return frame{}, err
	}
	control := opcode >= 0x8
	if control && (!fin || length > 125) {
		return frame{}, ErrProtocol
	}
	if length > uint64(c.cfg.MaxFrameBytes) {
		return frame{}, ErrMessageTooLarge
	}
	payload := make([]byte, int(length))
	var mask [4]byte
	if _, err := io.ReadFull(c.reader, mask[:]); err != nil {
		return frame{}, err
	}
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return frame{}, err
	}
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
	if opcode == byte(closeMessage) && len(payload) == 1 {
		return frame{}, ErrProtocol
	}
	if opcode == byte(closeMessage) && len(payload) >= 2 && !validCloseCode(closeCode(payload)) {
		return frame{}, ErrProtocol
	}
	if opcode == byte(closeMessage) && len(payload) > 2 && !utf8.Valid(payload[2:]) {
		return frame{}, ErrProtocol
	}
	return frame{opcode: opcode, fin: fin, payload: payload}, nil
}

func (c *Conn) readLength(indicator byte) (uint64, error) {
	if indicator <= 125 {
		return uint64(indicator), nil
	}
	switch indicator {
	case 126:
		var value [2]byte
		if _, err := io.ReadFull(c.reader, value[:]); err != nil {
			return 0, err
		}
		return uint64(binary.BigEndian.Uint16(value[:])), nil
	case 127:
		var value [8]byte
		if _, err := io.ReadFull(c.reader, value[:]); err != nil {
			return 0, err
		}
		length := binary.BigEndian.Uint64(value[:])
		if length&(1<<63) != 0 {
			return 0, ErrProtocol
		}
		if length > uint64(c.cfg.MaxFrameBytes) {
			return 0, ErrMessageTooLarge
		}
		return length, nil
	default:
		return 0, ErrProtocol
	}
}

// WriteMessage writes one complete server data message. It is bounded before
// touching the socket and serializes concurrent RPC/watch producers.
func (c *Conn) WriteMessage(ctx context.Context, messageType MessageType, data []byte) error {
	if messageType != TextMessage && messageType != BinaryMessage {
		return ErrProtocol
	}
	if int64(len(data)) > c.cfg.MaxMessageBytes || int64(len(data)) > c.cfg.MaxFrameBytes {
		return ErrMessageTooLarge
	}
	return c.write(ctx, byte(messageType), data)
}

func (c *Conn) write(ctx context.Context, opcode byte, payload []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.isClosed() {
		return ErrConnectionClosed
	}
	return c.writeLocked(ctx, opcode, payload, c.cfg.WriteTimeout)
}

// writeLocked owns both the frame and its cancellation deadline until the
// callback has stopped. A completed write must not expire a later close frame.
func (c *Conn) writeLocked(ctx context.Context, opcode byte, payload []byte, timeout time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := c.netConn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(callbackDone)
		_ = c.netConn.SetWriteDeadline(time.Now())
	})
	defer func() {
		if !stop() {
			<-callbackDone
		}
	}()
	if err := writeFrame(c.writer, opcode, payload); err != nil {
		return err
	}
	return c.writer.Flush()
}

func (c *Conn) writeControl(ctx context.Context, opcode byte, payload []byte) error {
	if len(payload) > 125 {
		return ErrProtocol
	}
	return c.write(ctx, opcode, payload)
}

func (c *Conn) writePong(ctx context.Context, payload []byte) error {
	return c.writeControl(ctx, byte(pongMessage), payload)
}

func (c *Conn) writeClose(ctx context.Context, code uint16, reason string) error {
	if !validCloseCode(code) || !utf8.ValidString(reason) {
		return ErrProtocol
	}
	reasonBytes := []byte(reason)
	if len(reasonBytes) > 123 {
		reasonBytes = reasonBytes[:123]
		for !utf8.Valid(reasonBytes) {
			reasonBytes = reasonBytes[:len(reasonBytes)-1]
		}
	}
	payload := make([]byte, 2+len(reasonBytes))
	binary.BigEndian.PutUint16(payload[:2], code)
	copy(payload[2:], reasonBytes)
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, closeWriteTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	// Close has stopped new data writes. Bound any in-flight Flush before
	// waiting for its mutex, then send the close frame if the peer drains it.
	// The same budget covers both the writer handoff and the close frame.
	deadline, _ := ctx.Deadline()
	if err := c.netConn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeLocked(ctx, byte(closeMessage), payload, closeWriteTimeout)
}

// Close sends a bounded close frame once and then releases the socket.
func (c *Conn) Close(ctx context.Context, code uint16, reason string) error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closed = true
	c.closeMu.Unlock()
	_ = c.writeClose(ctx, code, reason)
	return c.netConn.Close()
}

func (c *Conn) closeNetwork() error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closed = true
	c.closeMu.Unlock()
	return c.netConn.Close()
}

func (c *Conn) isClosed() bool {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	return c.closed
}

func writeFrame(w *bufio.ReadWriter, opcode byte, payload []byte) error {
	var header [10]byte
	header[0] = 0x80 | opcode
	var n int
	switch {
	case len(payload) <= 125:
		header[1] = byte(len(payload))
		n = 2
	case len(payload) <= 65535:
		header[1] = 126
		binary.BigEndian.PutUint16(header[2:4], uint16(len(payload)))
		n = 4
	default:
		header[1] = 127
		binary.BigEndian.PutUint64(header[2:10], uint64(len(payload)))
		n = 10
	}
	if _, err := w.Write(header[:n]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func closeCode(payload []byte) uint16 {
	if len(payload) < 2 {
		return 1000
	}
	return binary.BigEndian.Uint16(payload[:2])
}

func closeReason(payload []byte) string {
	if len(payload) <= 2 || !utf8.Valid(payload[2:]) {
		return ""
	}
	return string(payload[2:])
}

func validCloseCode(code uint16) bool {
	return code == 1000 || code == 1001 || code == 1002 || code == 1003 ||
		code == 1007 || code == 1008 || code == 1009 || code == 1010 ||
		code == 1011 || code == 1012 || code == 1013 || code == 1014
}

func websocketAccept(key string) string {
	h := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(h[:])
}
