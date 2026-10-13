package proxy

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	proxyDialer "golang.org/x/net/proxy"
)

const (
	// dialTimeout caps the TCP connect phase to the upstream proxy.
	dialTimeout = 3 * time.Second
	// connectRespTimeout caps the CONNECT/SOCKS5 handshake after the TCP connection is up.
	connectRespTimeout = 10 * time.Second
)

// connectViaSocks5 dials host through a SOCKS5 proxy.
// dialTimeout covers the TCP connect to the proxy; dialTimeout+connectRespTimeout
// covers the full SOCKS5 handshake via a context passed to DialContext.
func connectViaSocks5(p *models.Proxy, host string) (net.Conn, error) {
	var auth *proxyDialer.Auth
	if p.Username != nil && *p.Username != "" {
		pw := ""
		if p.Password != nil {
			pw = *p.Password
		}
		auth = &proxyDialer.Auth{User: *p.Username, Password: pw}
	}
	forward := &net.Dialer{Timeout: dialTimeout}
	dialer, err := proxyDialer.SOCKS5("tcp", p.Address, auth, forward)
	if err != nil {
		return nil, fmt.Errorf("socks5 dialer: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout+connectRespTimeout)
	defer cancel()
	if cd, ok := dialer.(proxyDialer.ContextDialer); ok {
		conn, err := cd.DialContext(ctx, "tcp", host)
		if err != nil {
			return nil, fmt.Errorf("socks5 dial %s via %s: %w", host, p.Address, err)
		}
		return conn, nil
	}
	conn, err := dialer.Dial("tcp", host)
	if err != nil {
		return nil, fmt.Errorf("socks5 dial %s via %s: %w", host, p.Address, err)
	}
	return conn, nil
}

// connectViaHTTPStandalone sends a CONNECT request to an HTTP proxy.
// dialTimeout covers the TCP connect; connectRespTimeout covers the CONNECT handshake.
func connectViaHTTPStandalone(p *models.Proxy, host string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", p.Address, dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial proxy %s: %w", p.Address, err)
	}

	_ = conn.SetDeadline(time.Now().Add(connectRespTimeout))

	req := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n", host, host)
	if p.Username != nil && *p.Username != "" {
		pw := ""
		if p.Password != nil {
			pw = *p.Password
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(*p.Username + ":" + pw))
		req += "Proxy-Authorization: Basic " + encoded + "\r\n"
	}
	req += "User-Agent: Rota-Proxy/1.0\r\nProxy-Connection: Keep-Alive\r\n\r\n"

	if _, err := conn.Write([]byte(req)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("send CONNECT to %s: %w", p.Address, err)
	}

	line, err := readCONNECTResponse(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read CONNECT response from %s: %w", p.Address, err)
	}
	if !strings.Contains(line, "200") {
		conn.Close()
		return nil, fmt.Errorf("CONNECT to %s rejected: %s", p.Address, line)
	}

	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

// readCONNECTResponse reads the upstream proxy's CONNECT reply up to the end of
// the header block (\r\n\r\n) WITHOUT consuming any bytes that belong to the
// tunnelled stream, and returns the status line.
//
// Reading into a large buffer (or via a bufio.Reader) can swallow the first
// bytes of the target server's TLS ServerHello when the proxy pipelines them
// right after the "200 Connection established" response — those bytes then
// never reach the client and the TLS handshake fails (issue #19). Since a
// CONNECT response has no body, the header terminator is a hard boundary, so we
// read one byte at a time until \r\n\r\n and stop exactly there. The response
// is tiny, so the extra syscalls are negligible.
func readCONNECTResponse(conn net.Conn) (string, error) {
	var buf []byte
	b := make([]byte, 1)
	for {
		n, err := conn.Read(b)
		if n > 0 {
			buf = append(buf, b[0])
			if l := len(buf); l >= 4 &&
				buf[l-4] == '\r' && buf[l-3] == '\n' &&
				buf[l-2] == '\r' && buf[l-1] == '\n' {
				break
			}
			if len(buf) > 8192 {
				return "", fmt.Errorf("CONNECT response headers too large")
			}
		}
		if err != nil {
			if len(buf) > 0 {
				break // return what we have; caller validates the status line
			}
			return "", err
		}
	}
	statusLine := string(buf)
	if idx := strings.Index(statusLine, "\r\n"); idx >= 0 {
		statusLine = statusLine[:idx]
	}
	return statusLine, nil
}
