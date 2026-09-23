package budget

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

var _ KeyWriter = (*redisWriter)(nil)

// redisWriter writes budget values to a Redis string key.
type redisWriter struct {
	client *goredis.Client
}

// NewRedisWriter returns a KeyWriter backed by the Redis server at addr.
func NewRedisWriter(addr string) KeyWriter {
	return &redisWriter{client: goredis.NewClient(&goredis.Options{Addr: addr})}
}

// SetKey writes value with no expiry. The absence of a TTL is deliberate: the
// consuming gate reads an absent key as full capacity, so a key that expires
// is indistinguishable from a key that says "dispatch freely".
func (w *redisWriter) SetKey(ctx context.Context, key, value string) error {
	if err := w.client.Set(ctx, key, value, 0).Err(); err != nil {
		return fmt.Errorf("redis SET %s: %w", key, err)
	}
	return nil
}

func (w *redisWriter) Close() error {
	if err := w.client.Close(); err != nil {
		return fmt.Errorf("failed to close redis client: %w", err)
	}
	return nil
}
