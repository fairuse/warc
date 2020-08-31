package warc

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

type Callback func(req *http.Request, resp *http.Response, reqData []byte, responseHeaders []byte)

type roundTripper struct {
	c *http.Client
}

type connWrapper struct {
	c    net.Conn
	resp *bytes.Buffer
	req  *bytes.Buffer
}

func (c *connWrapper) Read(b []byte) (n int, err error) {
	n, err = c.c.Read(b)

	c.resp.Write(b[:n])

	return
}

func (c *connWrapper) Write(b []byte) (n int, err error) {
	c.req.Write(b)

	return c.c.Write(b)
}

func (c *connWrapper) Close() error {
	return c.c.Close()
}

func (c *connWrapper) LocalAddr() net.Addr {
	return c.c.LocalAddr()
}

func (c *connWrapper) RemoteAddr() net.Addr {
	return c.c.RemoteAddr()
}

func (c *connWrapper) SetDeadline(t time.Time) error {
	return c.c.SetDeadline(t)
}

func (c *connWrapper) SetReadDeadline(t time.Time) error {
	return c.c.SetReadDeadline(t)
}

func (c *connWrapper) SetWriteDeadline(t time.Time) error {
	return c.c.SetWriteDeadline(t)
}

func foo(callback Callback) *http.Client {
	type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

	dT := http.DefaultTransport.(*http.Transport)
	cW := &connWrapper{
		req:  &bytes.Buffer{},
		resp: &bytes.Buffer{},
	}

	wrap := func(df DialFunc) DialFunc {
		return func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := df(ctx, network, addr)
			if err != nil {
				return nil, err
			}

			if cW.c != nil {
				panic(fmt.Errorf("connWrapper already contains a connection: want to wrap: %v; has %v", conn.RemoteAddr(), cW.c.RemoteAddr()))
			}

			cW.c = conn

			return cW, nil
		}
	}

	triggerCallback := func(req *http.Request, resp *http.Response) {
		callback(req, resp, cW.req.Bytes(), cW.resp.Bytes())
		cW.req.Reset()
		cW.resp.Reset()
	}

	// Represents http.defaultCheckRedirect
	checkRedirect := func(req *http.Request, via []*http.Request) error {
		triggerCallback(req, req.Response)

		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}

	transport := dT.Clone()
	transport.ForceAttemptHTTP2 = false
	transport.DisableCompression = true
	transport.DialContext = wrap(dT.DialContext)
	transport.DialTLSContext = wrap(func(ctx context.Context, network, addr string) (net.Conn, error) {
		tConfig := dT.TLSClientConfig.Clone()
		tConfig.NextProtos = []string{}

		dial, err := tls.Dial(network, addr, tConfig)

		if err != nil {
			return nil, err
		}
		return dial, dial.Handshake()
	})

	return &http.Client{
		Transport:     transport,
		CheckRedirect: checkRedirect,
	}
}

func NewRoundTripper(cb Callback) *roundTripper {
	return &roundTripper{
		c: foo(cb),
	}
}

func (t *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.c.Do(req)
	if err != nil {
		return nil, err
	}

	//triggerCallback(req, resp)

	return resp, err
}

var (
	_ http.RoundTripper = (*roundTripper)(nil)
)
