package httputil

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// MaxResponseBody is the maximum size of an HTTP response body (2 MB).
const MaxResponseBody = 2 * 1024 * 1024

// Shared HTTP transport with connection pooling for all demand adapters.
// Reuses TCP connections across adapters instead of each creating isolated pools.
var SharedTransport = &http.Transport{
	MaxIdleConns:        2000,
	MaxIdleConnsPerHost: 500,
	MaxConnsPerHost:     1000,
	IdleConnTimeout:     90 * time.Second,
}

// LimitedReadAll reads up to MaxResponseBody bytes from r.
func LimitedReadAll(r io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, MaxResponseBody))
}

// ReadResponseBody reads the response body, automatically decompressing gzip
// if the Content-Encoding header indicates it. Reads up to MaxResponseBody bytes.
func ReadResponseBody(resp *http.Response) ([]byte, error) {
	reader := bufio.NewReader(resp.Body)
	var ioReader io.Reader = reader

	// Peek 2 bytes to check for gzip magic number (0x1f 0x8b)
	peek, _ := reader.Peek(2)
	isGzip := len(peek) == 2 && peek[0] == 0x1f && peek[1] == 0x8b
	if isGzip || strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(reader)
		if err == nil {
			defer gz.Close()
			ioReader = gz
		}
	}
	body, err := io.ReadAll(io.LimitReader(ioReader, MaxResponseBody))
	if err != nil {
		return nil, err
	}
	return body, nil
}

// ResponseBodyReader returns an io.Reader for the response body that handles
// gzip decompression automatically. The caller must close the returned reader
// when done (if it's a gzip reader). Returns the reader and a close function.
func ResponseBodyReader(resp *http.Response) (io.Reader, func(), error) {
	reader := bufio.NewReader(resp.Body)

	// Peek 2 bytes for gzip
	peek, _ := reader.Peek(2)
	isGzip := len(peek) == 2 && peek[0] == 0x1f && peek[1] == 0x8b

	if isGzip || strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return nil, nil, err
		}
		return io.LimitReader(gz, MaxResponseBody), func() { gz.Close() }, nil
	}
	return io.LimitReader(reader, MaxResponseBody), func() {}, nil
}

// ValidateDemandURL checks that a URL is safe to use as a demand endpoint.
// Supports both HTTP and HTTPS schemes. Rejects loopback, link-local, and private addresses.
func ValidateDemandURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return net.InvalidAddrError("scheme must be http or https")
	}
	host := u.Hostname()
	if host == "" {
		return net.InvalidAddrError("empty host")
	}
	if isBlockedHost(host) {
		return net.InvalidAddrError("blocked destination")
	}
	return nil
}

func isBlockedHost(host string) bool {
	lower := strings.ToLower(host)
	if lower == "localhost" || strings.HasSuffix(lower, ".local") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// Hostname, not IP — allow (DNS resolution happens at connect time)
		return false
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast()
}

// NewClient creates an HTTP client that shares the global transport pool.
func NewClient(timeout time.Duration) *http.Client {
	if timeout == 0 {
		timeout = 800 * time.Millisecond
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: SharedTransport,
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}
}

// SetORTBHeaders sets the standard OpenRTB request headers.
func SetORTBHeaders(httpReq *http.Request, requestID, userAgent, clientIP, ortbVersion string) {
	ortbVersion = strings.TrimSpace(ortbVersion)
	if ortbVersion == "" {
		ortbVersion = "2.6"
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Openrtb-Version", ortbVersion)
	httpReq.Header.Set("X-Request-ID", requestID)

	if strings.TrimSpace(userAgent) == "" {
		userAgent = "ssp-ortb/" + ortbVersion
	}
	httpReq.Header.Set("User-Agent", userAgent)

	if ip := firstForwardedIP(clientIP); ip != "" {
		httpReq.Header.Set("X-Forwarded-For", ip)
	}

	// Ensure Host header matches the demand endpoint authority.
	if httpReq.URL != nil && httpReq.URL.Host != "" {
		httpReq.Host = httpReq.URL.Host
	}
}

func firstForwardedIP(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, ",")
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}

// JSONBufPool reuses byte buffers for JSON marshaling in the hot path.
var JSONBufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 2048))
	},
}

// GetBuffer returns a buffer from the pool, resetting it for reuse.
func GetBuffer() *bytes.Buffer {
	buf := JSONBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// PutBuffer returns a buffer to the pool.
func PutBuffer(buf *bytes.Buffer) {
	if buf.Cap() < 64*1024 { // Don't pool oversized buffers
		JSONBufPool.Put(buf)
	}
}
