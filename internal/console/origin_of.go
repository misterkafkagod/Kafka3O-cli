package console

import (
	"fmt"
	"net/url"
	"strings"
)

// OriginOf validates a console URL (http or https, no query or fragment) and returns it without a trailing slash
// together with its Origin (scheme://host[:port]), which the Console's front proxy checks on writes.
func OriginOf(consoleURL string) (base, origin string, err error) {
	parsed, err := url.Parse(strings.TrimSpace(consoleURL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", fmt.Errorf("invalid console URL %q: use http(s)://host[:port]", consoleURL)
	}
	return strings.TrimRight(parsed.Scheme+"://"+parsed.Host+parsed.EscapedPath(), "/"), parsed.Scheme + "://" + parsed.Host, nil
}
