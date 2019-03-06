package gonymizer

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestVersion(t *testing.T) {
	x := Version()
	assert.NotNil(t, x)
}

func TestBuildNumber(t *testing.T) {
	var tst int64
	x := BuildNumber()
	assert.IsType(t, x, tst)
}

func TestBuildDate(t *testing.T) {
	var tm time.Time
	x := BuildDate()
	assert.IsType(t, x, tm)
}
