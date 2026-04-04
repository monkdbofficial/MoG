package test

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// TestIntegration_Connection runs the corresponding test case.
func TestIntegration_Connection(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION") == "" {
		t.Skip("set RUN_INTEGRATION=1 to run integration test (requires MoG running)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uri := os.Getenv("MOG_URI")
	if uri == "" {
		uri = os.Getenv("MO2_URI") // backwards-compat
		if uri == "" {
			uri = os.Getenv("MONGO_PROXY_URI") // backwards-compat
		}
		if uri == "" {
			uri = "mongodb://127.0.0.1:27017"
		}
	}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("failed to connect to MoG: %v", err)
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		t.Fatalf("failed to ping MoG: %v", err)
	}

	t.Log("successfully connected to and pinged MoG")
}
