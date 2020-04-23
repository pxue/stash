package stash

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Stash struct {
	pool *redis.Pool
}

func NewDialer(host string) func() (redis.Conn, error) {
	return func() (redis.Conn, error) {
		return redis.Dial("tcp", host)
	}
}

func NewPool(dial func() (redis.Conn, error)) *redis.Pool {
	//TODO: make these configurable
	return &redis.Pool{
		MaxIdle:     10,
		MaxActive:   50,
		IdleTimeout: 300 * time.Second,
		Wait:        true,
		Dial:        dial,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func NewStash(host string) (*Stash, error) {
	pool := NewPool(NewDialer(host))

	conn := pool.Get()
	defer conn.Close()

	if _, err := conn.Do("PING"); err != nil {
		return nil, err
	}

	return &Stash{
		pool: pool,
	}, nil
}

func (s *Stash) Ping() error {
	conn := s.conn()
	defer conn.Close()

	reply, err := conn.Do("PING")
	if err != nil {
		return err
	}

	val, ok := reply.(string)
	if !ok || val != "PONG" {
		return errors.New("stash: service unreachable")
	}
	return nil
}

func (s *Stash) Get(bucket, key string) (interface{}, error) {
	return s.Do("HGET", bucket, key)
}

func (s *Stash) Set(bucket, key string, value interface{}) error {
	conn := s.conn()
	defer conn.Close()

	_, err := conn.Do("HSET", bucket, key, value)
	return err
}

func (s *Stash) Do(commandName string, args ...interface{}) (interface{}, error) {
	conn := s.conn()
	defer conn.Close()
	return conn.Do(commandName, args...)
}

func (s *Stash) conn() redis.Conn {
	return s.pool.Get()
}

func (s *Stash) buildKey(prefix string, id string, parts ...string) string {
	key := fmt.Sprintf("%s:%s", prefix, id)
	if len(parts) > 0 {
		return key + ":" + strings.Join(parts, ":")
	} else {
		return key
	}
}

func (s *Stash) bucketKey(bucketId string, parts ...string) string {
	return s.buildKey("bucket", bucketId, parts...)
}
