package mongo

import (
	"fmt"

	"github.com/xdg-go/scram"
)

var SHA256 scram.HashGeneratorFcn = scram.SHA256

// ScramSha256 handles the server-side SCRAM-SHA-256 authentication flow.
type ScramSha256 struct {
	server *scram.Server
}

// NewScramSha256 creates a new SCRAM-SHA-256 server-side authenticator.
func NewScramSha256(user, pass string) (*ScramSha256, error) {
	// In a real application, this would look up users in a database.
	credStore := make(map[string]scram.StoredCredentials)

	client, err := SHA256.NewClient(user, pass, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create scram client for storing credentials: %w", err)
	}
	// The salt should be randomly generated and stored. For this example, we use a fixed salt.
	// The iteration count should be as high as you can tolerate.
	storedCreds, err := client.GetStoredCredentialsWithError(scram.KeyFactors{Salt: "somesalt", Iters: 4096})
	if err != nil {
		return nil, fmt.Errorf("failed to get stored credentials: %w", err)
	}
	credStore[user] = storedCreds

	server, err := SHA256.NewServer(func(userName string) (scram.StoredCredentials, error) {
		creds, ok := credStore[userName]
		if !ok {
			return scram.StoredCredentials{}, fmt.Errorf("user %q not found", userName)
		}
		return creds, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create scram server: %w", err)
	}

	return &ScramSha256{
		server: server,
	}, nil
}

// Conversation represents a single SCRAM authentication exchange.
type Conversation struct {
	conv *scram.ServerConversation
}

// Start begins a new authentication conversation.
func (s *ScramSha256) Start() *Conversation {
	return &Conversation{
		conv: s.server.NewConversation(),
	}
}

// Step processes a message from the client and returns the server's response.
func (c *Conversation) Step(clientMsg string) (string, error) {
	return c.conv.Step(clientMsg)
}

// Done returns true if the authentication exchange is complete.
func (c *Conversation) Done() bool {
	return c.conv.Done()
}
