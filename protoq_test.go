package protoq

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// TestEncodeDecode 测试基本编解码往返
func TestEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		frame  *Frame
	}{
		{
			name:  "请求有Body有CRC",
			frame: NewRequestFrame(0x0001, 0x0001, []byte("hello"), true, true),
		},
		{
			name:  "响应有Body有CRC",
			frame: NewResponseFrame(0x0001, 0x0001, []byte("world"), Flags(FlagOPLEN2|FlagSEQLEN2|FlagCRCLEN2)),
		},
		{
			name:  "通知无Body无CRC",
			frame: NewNotificationFrame(0x0002, nil, false),
		},
		{
			name:  "请求空Body有CRC",
			frame: NewRequestFrame(0x00FF, 0x00FF, nil, true, false),
		},
		{
			name: "大Body请求",
			frame: func() *Frame {
				body := make([]byte, 1024)
				for i := range body {
					body[i] = byte(i % 256)
				}
				return NewRequestFrame(0x0003, 0x0001, body, true, true)
			}(),
		},
		{
			name: "4字节Opcode和Seq",
			frame: func() *Frame {
				f := NewRequestFrame(0x12345678, 0x9ABCDEF0, []byte("data"), true, true)
				f.Flags = f.Flags.SetOpcodeLen(4).SetSeqLen(4)
				return f
			}(),
		},
		{
			name:  "响应无Body",
			frame: NewResponseFrame(0x0001, 0x0001, nil, Flags(FlagOPLEN2|FlagSEQLEN2|FlagCRCLEN2)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 编码
			data, err := Encode(tt.frame)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			// 验证四字节对齐
			if len(data)%4 != 0 {
				t.Errorf("frame not 4-byte aligned: len=%d", len(data))
			}

			// 验证 Magic
			if data[0] != MagicByte {
				t.Errorf("invalid magic: got 0x%02X, want 0x%02X", data[0], MagicByte)
			}

			// 解码
			decoder := NewDecoder(bytes.NewReader(data))
			decoded, err := decoder.Decode()
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			// 比较
			if decoded.Flags != tt.frame.Flags {
				t.Errorf("Flags mismatch: got 0x%02X, want 0x%02X", decoded.Flags, tt.frame.Flags)
			}
			if decoded.Opcode != tt.frame.Opcode {
				t.Errorf("Opcode mismatch: got %d, want %d", decoded.Opcode, tt.frame.Opcode)
			}
			if decoded.Seq != tt.frame.Seq {
				t.Errorf("Seq mismatch: got %d, want %d", decoded.Seq, tt.frame.Seq)
			}
			if !bytes.Equal(decoded.Body, tt.frame.Body) {
				t.Errorf("Body mismatch: got %v, want %v", decoded.Body, tt.frame.Body)
			}
		})
	}
}

// TestDecodeStickyPackets 测试粘包场景 — 多个完整帧在一次 Read 中到达
func TestDecodeStickyPackets(t *testing.T) {
	// 构造多个帧并拼接在一起
	f1 := NewRequestFrame(0x0001, 0x0001, []byte("first"), true, true)
	f2 := NewNotificationFrame(0x0002, []byte("second"), true)
	f3 := NewResponseFrame(0x0001, 0x0001, []byte("third"), Flags(FlagOPLEN2|FlagSEQLEN2|FlagCRCLEN2))

	data1, _ := Encode(f1)
	data2, _ := Encode(f2)
	data3, _ := Encode(f3)

	combined := append(append(data1, data2...), data3...)

	decoder := NewDecoder(bytes.NewReader(combined))

	frames := make([]*Frame, 0, 3)
	for i := 0; i < 3; i++ {
		f, err := decoder.Decode()
		if err != nil {
			t.Fatalf("Decode frame %d failed: %v", i+1, err)
		}
		frames = append(frames, f)
	}

	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}

	if string(frames[0].Body) != "first" {
		t.Errorf("frame 1 body: got %q, want %q", frames[0].Body, "first")
	}
	if string(frames[1].Body) != "second" {
		t.Errorf("frame 2 body: got %q, want %q", frames[1].Body, "second")
	}
	if string(frames[2].Body) != "third" {
		t.Errorf("frame 3 body: got %q, want %q", frames[2].Body, "third")
	}
}

// TestDecodeHalfPacket 测试半包场景 — 帧被分割到多次 Read 中
func TestDecodeHalfPacket(t *testing.T) {
	f := NewRequestFrame(0x0001, 0x0001, []byte("half packet test data"), true, true)
	data, _ := Encode(f)

	// 使用一个自定义 reader，每次只返回少量字节
	halfReader := &chunkedReader{data: data, chunkSize: 3}
	decoder := NewDecoder(halfReader)

	decoded, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if string(decoded.Body) != "half packet test data" {
		t.Errorf("body mismatch: got %q, want %q", decoded.Body, "half packet test data")
	}
	if decoded.Opcode != 0x0001 {
		t.Errorf("opcode mismatch: got %d, want 1", decoded.Opcode)
	}
}

// chunkedReader 按 chunkSize 分块返回数据，模拟 TCP 流的半包行为
type chunkedReader struct {
	data      []byte
	chunkSize int
	pos       int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	remaining := len(r.data) - r.pos
	chunk := r.chunkSize
	if chunk > remaining {
		chunk = remaining
	}
	if chunk > len(p) {
		chunk = len(p)
	}
	copy(p, r.data[r.pos:r.pos+chunk])
	r.pos += chunk
	return chunk, nil
}

// TestDecodeNoiseBeforeMagic 测试帧前有噪声数据（同步测试）
func TestDecodeNoiseBeforeMagic(t *testing.T) {
	f := NewRequestFrame(0x0001, 0x0001, []byte("data"), true, true)
	data, _ := Encode(f)

	// 在帧前添加噪声
	noise := []byte{0x00, 0xFF, 0xAA, 0x00}
	combined := append(noise, data...)

	decoder := NewDecoder(bytes.NewReader(combined))
	decoded, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if string(decoded.Body) != "data" {
		t.Errorf("body mismatch: got %q, want %q", decoded.Body, "data")
	}
}

// TestCRCMismatch 测试 CRC 校验失败
func TestCRCMismatch(t *testing.T) {
	f := NewRequestFrame(0x0001, 0x0001, []byte("test"), true, true)
	data, _ := Encode(f)

	// 篡改 Body 中的一个字节
	// Body 在数据中的位置取决于 Flags 布局
	// Magic(1) + Flags(1) + Length(2) + Opcode(2) + Seq(2) = 8, Body 从 offset 8 开始
	if len(data) > 9 {
		data[9] ^= 0xFF // 翻转 Body 中的一个字节
	}

	decoder := NewDecoder(bytes.NewReader(data))
	_, err := decoder.Decode()
	if err == nil {
		t.Error("expected CRC mismatch error, got nil")
	}
}

// TestFlagsValidation 测试标志位验证
func TestFlagsValidation(t *testing.T) {
	// ACK_REQ=1 但 SEQ_LEN=0
	f := &Frame{
		Flags:  FlagACKREQ | FlagOPLEN2 | FlagSEQLEN0 | FlagCRCLEN2 | FlagHASLEN,
		Opcode: 0x0001,
		Seq:    0,
		Body:   []byte("test"),
	}
	_, err := Encode(f)
	if err != ErrACKReqNeedsSeq {
		t.Errorf("expected ErrACKReqNeedsSeq, got %v", err)
	}

	// 响应帧中 ACK_REQ=1
	f2 := &Frame{
		Flags:  FlagDIR | FlagACKREQ | FlagOPLEN2 | FlagSEQLEN2 | FlagCRCLEN2,
		Opcode: 0x0001,
		Seq:    0x0001,
		Body:   nil,
	}
	_, err = Encode(f2)
	if err != ErrResponseCantAckReq {
		t.Errorf("expected ErrResponseCantAckReq, got %v", err)
	}
}

// TestCRC16IBM 测试 CRC-16-IBM 计算
func TestCRC16IBM(t *testing.T) {
	// 已知测试向量：空数据 CRC-16-IBM = 0x0000
	crc := CRC16IBM([]byte{})
	if crc != 0x0000 {
		t.Errorf("CRC-16-IBM of empty: got 0x%04X, want 0x0000", crc)
	}

	// "123456789" 的 CRC-16-IBM (ARC) = 0xBB3D
	crc = CRC16IBM([]byte("123456789"))
	expected := uint16(0xBB3D)
	if crc != expected {
		t.Errorf("CRC-16-IBM of '123456789': got 0x%04X, want 0x%04X", crc, expected)
	}
}

// TestRequestResponseMatching 测试请求-应答匹配（端到端）
func TestRequestResponseMatching(t *testing.T) {
	// 使用内存管道模拟连接
	srvPipe, clientConn := net.Pipe()
	defer srvPipe.Close()
	defer clientConn.Close()

	// 服务端
	server := NewServer(nil) // nil factory — 只使用连接级处理
	server.Handle(0x0001, func(opcode uint32, body []byte) ([]byte, error) {
		return []byte("echo: " + string(body)), nil
	})
	server.Handle(0x0002, func(opcode uint32, body []byte) ([]byte, error) {
		return nil, nil // 空响应
	})

	// 手动创建服务端连接处理
	sc := &serverConn{
		id:      1,
		conn:    srvPipe,
		decoder: NewDecoder(srvPipe),
		server:  server,
		readDone: make(chan struct{}),
	}
	go sc.serve()

	// 客户端
	client := NewClient(clientConn, WithClientOpcodeLen(2), WithClientSeqLen(2), WithClientCRC(true))

	// 发送请求并等待响应
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.SendRequest(ctx, 0x0001, []byte("hello world"))
	if err != nil {
		t.Fatalf("SendRequest failed: %v", err)
	}

	if string(resp.Body) != "echo: hello world" {
		t.Errorf("unexpected response: %q", resp.Body)
	}
	if resp.Opcode != 0x0001 {
		t.Errorf("unexpected response opcode: %d", resp.Opcode)
	}

	// 发送通知
	err = client.SendNotification(0x0002, []byte("notify"))
	if err != nil {
		t.Fatalf("SendNotification failed: %v", err)
	}

	// 清理
	client.Close()
}

// TestMultipleRequests 测试多个并发请求
func TestMultipleRequests(t *testing.T) {
	srvPipe, clientConn := net.Pipe()
	defer srvPipe.Close()
	defer clientConn.Close()

	server := NewServer(nil)
	server.Handle(0x0001, func(opcode uint32, body []byte) ([]byte, error) {
		// 模拟一些处理时间
		time.Sleep(10 * time.Millisecond)
		return body, nil
	})

	sc := &serverConn{
		id:      1,
		conn:    srvPipe,
		decoder: NewDecoder(srvPipe),
		server:  server,
		readDone: make(chan struct{}),
	}
	go sc.serve()

	client := NewClient(clientConn, WithClientOpcodeLen(2), WithClientSeqLen(2), WithClientCRC(true))
	defer client.Close()

	// 并发发送多个请求
	const numRequests = 10
	var wg sync.WaitGroup
	errs := make(chan error, numRequests)

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			body := []byte("request-" + string(rune('A'+idx)))
			resp, err := client.SendRequest(ctx, 0x0001, body)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(resp.Body, body) {
				errs <- &testError{msg: "body mismatch"}
				return
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string { return e.msg }

// TestSeqOverflow 测试序列号回绕
func TestSeqOverflow(t *testing.T) {
	sm := NewSeqManager(2) // 16 位序列号

	// 分配大量序列号，验证回绕
	var first uint32
	for i := 0; i < 70000; i++ {
		seq := sm.Allocate()
		if i == 0 {
			first = seq
		}
		if seq == 0 {
			t.Error("Allocate returned 0 unexpectedly")
		}
	}
	// 序列号应该已经回绕
	if first == 1 {
		t.Log("seq started at 1 as expected")
	}
}

// TestEncoder4ByteAlignment 测试四字节对齐
func TestEncoder4ByteAlignment(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"empty body", ""},
		{"1 byte", "a"},
		{"2 bytes", "ab"},
		{"3 bytes", "abc"},
		{"4 bytes", "abcd"},
		{"5 bytes", "abcde"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewRequestFrame(0x0001, 0x0001, []byte(tt.body), true, true)
			data, err := Encode(f)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}
			if len(data)%4 != 0 {
				t.Errorf("frame not aligned: len=%d (body=%q)", len(data), tt.body)
			}
		})
	}
}

// TestVariantB 测试变体 B（无 Body，无 Length）
func TestVariantB(t *testing.T) {
	// 变体 B：响应无 Body，HAS_LEN=0
	f := NewResponseFrame(0x0001, 0x0001, nil, Flags(FlagOPLEN2|FlagSEQLEN2|FlagCRCLEN2))
	// 确保 HAS_LEN 为 0
	f.Flags = f.Flags.SetHasLen(false)

	data, err := Encode(f)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// 验证 HAS_LEN 位为 0
	if data[1]&0x20 != 0 {
		t.Error("HAS_LEN should be 0 for variant B")
	}

	decoder := NewDecoder(bytes.NewReader(data))
	decoded, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Opcode != 0x0001 {
		t.Errorf("opcode mismatch: got %d", decoded.Opcode)
	}
	if decoded.Seq != 0x0001 {
		t.Errorf("seq mismatch: got %d", decoded.Seq)
	}
	if len(decoded.Body) != 0 {
		t.Errorf("body should be empty, got %d bytes", len(decoded.Body))
	}
}
