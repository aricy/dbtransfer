package main

import "time"

type Checkpoint struct {
	LastKey     map[string]string `json:"last_key"`
	LastUpdated time.Time         `json:"last_updated"`
	Complete    bool              `json:"complete"`
}
