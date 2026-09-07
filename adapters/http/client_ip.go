package ratelimithttp

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// ErrInvalidClientIP identifies an unsafe peer address or forwarded chain.
var ErrInvalidClientIP = errors.New("invalid client IP")

// MaxTrustedProxies bounds trusted-proxy configuration.
const MaxTrustedProxies = 64
const maxForwardedBytes, maxForwardedHops = 4096, 32

// ClientIPOptions defines the peers trusted to supply X-Forwarded-For.
type ClientIPOptions struct {
	// TrustedProxies contains copied, validated network prefixes.
	TrustedProxies []netip.Prefix
}

// ClientIPExtractor derives client addresses from trusted proxy chains.
type ClientIPExtractor struct{ trusted []netip.Prefix }

// NewClientIPExtractor validates and copies trusted proxy prefixes.
func NewClientIPExtractor(options ClientIPOptions) (*ClientIPExtractor, error) {
	if len(options.TrustedProxies) > MaxTrustedProxies {
		return nil, fmt.Errorf("%w: too many trusted proxies", ErrInvalidClientIP)
	}
	trusted := make([]netip.Prefix, len(options.TrustedProxies))
	for index, prefix := range options.TrustedProxies {
		if !prefix.IsValid() {
			return nil, fmt.Errorf("%w: trusted proxy at index %d is invalid", ErrInvalidClientIP, index)
		}
		trusted[index] = prefix.Masked()
	}
	return &ClientIPExtractor{trusted: trusted}, nil
}

// ClientIP returns the rightmost untrusted address, or the direct peer.
func (extractor *ClientIPExtractor) ClientIP(request *http.Request) (netip.Addr, error) {
	if extractor == nil || extractor.trusted == nil {
		return netip.Addr{}, fmt.Errorf("%w: extractor is nil or uninitialized", ErrInvalidClientIP)
	}
	if request == nil {
		return netip.Addr{}, fmt.Errorf("%w: request is required", ErrInvalidClientIP)
	}
	peer, err := parseRemoteAddr(request.RemoteAddr)
	if err != nil || !extractor.isTrusted(peer) {
		return peer, err
	}
	forwarded := request.Header.Get("X-Forwarded-For")
	if forwarded == "" {
		return peer, nil
	}
	if len(forwarded) > maxForwardedBytes {
		return netip.Addr{}, fmt.Errorf("%w: forwarded chain too large", ErrInvalidClientIP)
	}
	parts := strings.Split(forwarded, ",")
	if len(parts) > maxForwardedHops {
		return netip.Addr{}, fmt.Errorf("%w: too many forwarded hops", ErrInvalidClientIP)
	}
	addresses := make([]netip.Addr, 0, len(parts))
	for _, part := range parts {
		address, parseErr := netip.ParseAddr(strings.TrimSpace(part))
		if parseErr != nil {
			return netip.Addr{}, fmt.Errorf("%w: malformed forwarded hop", ErrInvalidClientIP)
		}
		addresses = append(addresses, address.Unmap())
	}
	for offset := range len(addresses) {
		index := len(addresses) - 1 - offset
		if !extractor.isTrusted(addresses[index]) {
			return addresses[index], nil
		}
	}
	return addresses[0], nil
}

func (extractor *ClientIPExtractor) isTrusted(address netip.Addr) bool {
	for _, prefix := range extractor.trusted {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

func parseRemoteAddr(value string) (netip.Addr, error) {
	host, _, err := net.SplitHostPort(value)
	if err != nil {
		host = value
	}
	address, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("%w: malformed remote address", ErrInvalidClientIP)
	}
	return address.Unmap(), nil
}
