package concurrency

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

// ErrNoSlot defines the error when beyond concurrency
var ErrNoSlot = errors.New("beyond concurrency")

// RedisConnector contains all function to access redis
type RedisConnector interface {
	MGet(ctx context.Context, keys []string) ([]string, error)
	Get(ctx context.Context, key string) (string, error)
	Del(ctx context.Context, keys ...string) error
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
}

// RateLimiter defines the concurrency job limiter
type RateLimiter struct {
	redisConnector RedisConnector
	defaultTTL     time.Duration
}

// GenJobKeys generates job keys by job type and limit
func (rl *RateLimiter) GenJobKeys(jobType string, limit int) []string {
	slotKeys := make([]string, limit)
	for i := 0; i < limit; i++ {
		slotKeys[i] = fmt.Sprintf("%s-%d", jobType, i)
	}

	return slotKeys
}

// AddJob adds a new job, if all slots are taken, an error will be return
func (rl *RateLimiter) AddJob(jobType string, limit int, jobID string, ttl time.Duration) (string, error) {
	slots, err := rl.ListJobs(jobType, limit)
	if err != nil {
		return "", err
	}

	if jobID != "" {
		jobID = uuid.NewString()
	}
	findASlot := false
	for k, slot := range slots {
		if slot != "" {
			continue
		}
		if ttl == 0 {
			ttl = rl.defaultTTL
		}
		if err := rl.redisConnector.Set(context.TODO(), k, jobID, ttl); err != nil {
			return "", err
		}
		findASlot = true
		break
	}
	if findASlot {
		return jobID, nil
	}

	return "", ErrNoSlot
}

// ListJobs return all active jobs with map[string]string format
func (rl *RateLimiter) ListJobs(jobType string, limit int) (map[string]string, error) {
	result := map[string]string{}
	slotKeys := rl.GenJobKeys(jobType, limit)

	values, err := rl.redisConnector.MGet(context.TODO(), slotKeys)
	if err != nil {
		return nil, err
	}
	for i, value := range values {
		result[slotKeys[i]] = value
	}

	return result, nil
}

// DeleteJob deletes a job by its jobID
func (rl *RateLimiter) DeleteJob(jobType string, limit int, jobID string) error {
	slots, err := rl.ListJobs(jobType, limit)
	if err != nil {
		return err
	}

	for k, v := range slots {
		if v != jobID {
			continue
		}
		if err := rl.redisConnector.Del(context.TODO(), k); err != nil {
			return err
		}
	}

	return nil
}

// Redis defines a wrapper of go-redis
// The API is set with chaining style, so the commands cannot be used directly
type Redis struct {
	Client *redis.Client
}

// NewRedis is the constructor of Redis
// connects to redis host
func NewRedis(options *redis.Options) *Redis {
	return &Redis{
		Client: redis.NewClient(options),
	}
}

// Get wraps redis.Get
func (r *Redis) Get(ctx context.Context, key string) (string, error) {
	return r.Client.Get(ctx, key).Result()
}

// MGet wraps redis.MGet
func (r *Redis) MGet(ctx context.Context, keys ...string) ([]string, error) {
	values, err := r.Client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	result := make([]string, len(values))
	for i, value := range values {
		if s, ok := value.(string); ok {
			result[i] = s
		} else {
			return nil, errors.New("invalid type")
		}
	}

	return result, nil
}

// Del wraps redis.Del
func (r *Redis) Del(ctx context.Context, keys ...string) error {
	return r.Client.Del(ctx, keys...).Err()
}

// Set wraps redis.Set
func (r *Redis) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	return r.Client.Set(ctx, key, value, ttl).Err()
}
