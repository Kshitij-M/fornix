package credentials

import (
	"errors"
	"net/http"
)

var ErrCredentialRequestInvalid = errors.New("credential request is not configured")

// ClearAuthorizationHeader removes a credential from an owned request after
// the transport returns. Go strings cannot be reliably zeroed, but dropping
// the request's reference shortens accidental retention by callers and test
// transports.
func ClearAuthorizationHeader(request *http.Request) {
	if request == nil || request.Header == nil {
		return
	}
	request.Header.Del("Authorization")
}

// DoCredentialRequest sends a request and removes its Authorization header on
// every return path. It does not claim to zero immutable Go strings or copies
// retained by a custom transport.
func DoCredentialRequest(client *http.Client, request *http.Request) (*http.Response, error) {
	if client == nil || request == nil {
		return nil, ErrCredentialRequestInvalid
	}
	defer ClearAuthorizationHeader(request)
	return client.Do(request)
}
