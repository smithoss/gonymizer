package gonymizer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVersion(t *testing.T) {
	x := Version()
	require.NotNil(t, x)
}

func TestBuildNumber(t *testing.T) {
	var tst int64
	x := BuildNumber()
	require.IsType(t, x, tst)
}

func TestBuildDate(t *testing.T) {
	var tm time.Time
	x := BuildDate()
	require.IsType(t, x, tm)
}
