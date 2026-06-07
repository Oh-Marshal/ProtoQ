package transport

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// WebSocket 帧操作码
const (
	wsTextFrame   = 1
	wsBinaryFrame = 2
	wsCloseFrame  = 8
	wsPingFrame   = 9
	wsPongFrame   = 10
)

// wsConn 是对 net.Conn 的 WebSocket 帧封装。
// 在原始 TCP 连接之上提供 WebSocket 消息语义（自动处理帧头/掩码）。
type wsConn struct {
	conn   net.Conn
	reader io.Reader // 可能是 bufio.Reader
	writer io.Writer
	mu     sync.Mutex // 保护写入
	closed bool
}

// Read 从 WebSocket 连接读取解帧后的数据。
func (w *wsConn) Read(p []byte) (int, error) {
	for {
		// 读取帧头
		header := make([]byte, 2)
		if _, err := io.ReadFull(w.reader, header); err != nil {
			return 0, err
		}

		opcode := header[0] & 0x0F
		masked := header[1]&0x80 != 0

		payloadLen := uint64(header[1] & 0x7F)

		// 扩展长度
		switch payloadLen {
		case 126:
			ext := make([]byte, 2)
			if _, err := io.ReadFull(w.reader, ext); err != nil {
				return 0, err
			}
			payloadLen = uint64(binary.BigEndian.Uint16(ext))
		case 127:
			ext := make([]byte, 8)
			if _, err := io.ReadFull(w.reader, ext); err != nil {
				return 0, err
			}
			payloadLen = binary.BigEndian.Uint64(ext)
		}

		// 掩码密钥
		var maskKey [4]byte
		if masked {
			if _, err := io.ReadFull(w.reader, maskKey[:]); err != nil {
				return 0, err
			}
		}

		// 处理控制帧
		switch opcode {
		case wsCloseFrame:
			return 0, io.EOF
		case wsPingFrame:
			// 读取 ping 数据并发送 pong
			pingData := make([]byte, payloadLen)
			if _, err := io.ReadFull(w.reader, pingData); err != nil {
				return 0, err
			}
			if masked {
				maskBytes(pingData, maskKey)
			}
			w.writeFrame(wsPongFrame, pingData)
			continue
		case wsPongFrame:
			// 忽略 pong
			if payloadLen > 0 {
				pongData := make([]byte, payloadLen)
				if _, err := io.ReadFull(w.reader, pongData); err != nil {
					return 0, err
				}
			}
			continue
		case wsBinaryFrame, wsTextFrame:
			// 数据帧
			if payloadLen > uint64(len(p)) {
				// 缓冲区太小，分段读取
				remaining := payloadLen
				totalRead := 0
				buf := make([]byte, 4096)
				for remaining > 0 {
					toRead := buf
					if remaining < uint64(len(buf)) {
						toRead = buf[:remaining]
					}
					n, err := io.ReadFull(w.reader, toRead)
					if err != nil {
						return totalRead, err
					}
					if masked {
						maskBytes(toRead[:n], maskKey)
						// 旋转掩码密钥（理论上 RFC 要求连续掩码，但实际实现中我们分段应用）
						// 简化处理：对整个数据使用相同掩码
					}
					copy(p[totalRead:], toRead[:n])
					totalRead += n
					remaining -= uint64(n)
				}
				return totalRead, nil
			}

			data := make([]byte, payloadLen)
			if _, err := io.ReadFull(w.reader, data); err != nil {
				return 0, err
			}
			if masked {
				maskBytes(data, maskKey)
			}
			n := copy(p, data)
			return n, nil
		default:
			// 未知操作码，跳过
			if payloadLen > 0 {
				discard := make([]byte, payloadLen)
				io.ReadFull(w.reader, discard)
			}
			continue
		}
	}
}

// Write 将数据以 WebSocket 二进制帧发送。
func (w *wsConn) Write(p []byte) (int, error) {
	if err := w.writeFrame(wsBinaryFrame, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// writeFrame 写入一个 WebSocket 帧。
func (w *wsConn) writeFrame(opcode byte, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// FIN + opcode
	frame := []byte{0x80 | opcode}

	// 长度编码
	payloadLen := len(payload)
	switch {
	case payloadLen <= 125:
		frame = append(frame, byte(payloadLen))
	case payloadLen <= 65535:
		frame = append(frame, 126)
		ext := make([]byte, 2)
		binary.BigEndian.PutUint16(ext, uint16(payloadLen))
		frame = append(frame, ext...)
	default:
		frame = append(frame, 127)
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(payloadLen))
		frame = append(frame, ext...)
	}

	// 服务端→客户端帧不需要掩码
	frame = append(frame, payload...)

	_, err := w.conn.Write(frame)
	return err
}

// Close 发送关闭帧并关闭连接。
func (w *wsConn) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()

	// 发送关闭帧
	w.writeFrame(wsCloseFrame, nil)
	return w.conn.Close()
}

// LocalAddr 返回本地地址。
func (w *wsConn) LocalAddr() net.Addr { return w.conn.LocalAddr() }

// RemoteAddr 返回远程地址。
func (w *wsConn) RemoteAddr() net.Addr { return w.conn.RemoteAddr() }

// SetDeadline 设置读写的截止时间。
func (w *wsConn) SetDeadline(t time.Time) error { return w.conn.SetDeadline(t) }

// SetReadDeadline 设置读取的截止时间。
func (w *wsConn) SetReadDeadline(t time.Time) error { return w.conn.SetReadDeadline(t) }

// SetWriteDeadline 设置写入的截止时间。
func (w *wsConn) SetWriteDeadline(t time.Time) error { return w.conn.SetWriteDeadline(t) }

// maskBytes 对数据应用 WebSocket 掩码。
func maskBytes(data []byte, key [4]byte) {
	for i := range data {
		data[i] ^= key[i%4]
	}
}

// wsListener 对 net.Listener 进行包装，Accept 时自动完成 WebSocket 升级。
type wsListener struct {
	listener net.Listener
}

// Accept 接受新连接并完成 WebSocket 升级握手。
func (l *wsListener) Accept() (net.Conn, error) {
	conn, err := l.listener.Accept()
	if err != nil {
		return nil, err
	}

	// 读取 HTTP 升级请求
	br := bufio.NewReader(conn)
	req, err := http.ReadRequest(br)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("ws: read upgrade request: %w", err)
	}

	// 验证 WebSocket 升级头
	key := req.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		conn.Close()
		return nil, errors.New("ws: missing Sec-WebSocket-Key")
	}

	// 计算 Accept 密钥
	acceptKey := computeAcceptKey(key)

	// 发送 101 响应
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptKey + "\r\n" +
		"\r\n"
	if _, err := conn.Write([]byte(response)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ws: write upgrade response: %w", err)
	}

	return &wsConn{conn: conn, reader: conn, writer: conn}, nil
}

// Close 关闭监听器。
func (l *wsListener) Close() error {
	return l.listener.Close()
}

// Addr 返回监听地址。
func (l *wsListener) Addr() net.Addr {
	return l.listener.Addr()
}

// computeAcceptKey 计算 Sec-WebSocket-Accept 值。
func computeAcceptKey(key string) string {
	const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	h := sha1.New()
	h.Write([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// generateWebSocketKey 生成随机的 Sec-WebSocket-Key。
func generateWebSocketKey() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// WSTransport 基于 WebSocket 的传输实现。
// 客户端通过 HTTP Upgrade 建立 WebSocket 连接。
type WSTransport struct {
	// Path 是 WebSocket 升级请求的路径（默认 "/"）。
	Path string
}

// NewWSTransport 创建一个 WebSocket 传输实例。
func NewWSTransport() *WSTransport {
	return &WSTransport{Path: "/"}
}

// Dial 通过 HTTP Upgrade 建立 WebSocket 连接。
func (t *WSTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	// 生成随机密钥
	key, err := generateWebSocketKey()
	if err != nil {
		return nil, fmt.Errorf("ws: generate key: %w", err)
	}

	// 建立 TCP 连接
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("ws: dial: %w", err)
	}

	// 发送 HTTP Upgrade 请求
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	path := t.Path
	if path == "" {
		path = "/"
	}

	req := fmt.Sprintf(
		"GET %s HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n"+
			"\r\n",
		path, host, key)

	if _, err := conn.Write([]byte(req)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ws: write upgrade request: %w", err)
	}

	// 读取响应
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("ws: read upgrade response: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 101 {
		conn.Close()
		return nil, fmt.Errorf("ws: upgrade failed, status %d", resp.StatusCode)
	}

	// 验证 Accept 密钥
	expectedAccept := computeAcceptKey(key)
	actualAccept := resp.Header.Get("Sec-WebSocket-Accept")
	if !strings.EqualFold(expectedAccept, actualAccept) {
		conn.Close()
		return nil, errors.New("ws: Sec-WebSocket-Accept mismatch")
	}

	return &wsConn{conn: conn, reader: conn, writer: conn}, nil
}

// Listen 使用 WebSocket 在指定地址上监听。
func (t *WSTransport) Listen(ctx context.Context, addr string) (net.Listener, error) {
	var lc net.ListenConfig
	rawListener, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return &wsListener{listener: rawListener}, nil
}

// String 返回传输协议名称。
func (t *WSTransport) String() string {
	return "ws"
}
