package sqs

import (
	"github.com/gofiber/fiber/v2"
	"time"
)

// HTTPConfig also protects the outer listener in single-port mode. The write
// budget includes the maximum 20-second long poll plus storage/network overhead.
func HTTPConfig(bodyLimit int) fiber.Config {
	if bodyLimit <= 0 || bodyLimit > 8<<20 {
		bodyLimit = 8 << 20
	}
	return fiber.Config{
		DisableStartupMessage: true,
		BodyLimit:             bodyLimit,
		Concurrency:           32,
		ReadTimeout:           10 * time.Second,
		WriteTimeout:          35 * time.Second,
		IdleTimeout:           30 * time.Second,
		ReduceMemoryUsage:     true,
	}
}
