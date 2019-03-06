package gonymizer

import "time"

const (
	internal_BUILD_TIMESTAMP = 1549922253
	internal_BUILD_NUMBER    = 1
	internal_VERSION_STRING  = "1.0.0"
)

// BuildDate will return the current unix time as the build date time for the application.
func BuildDate() time.Time {
	return time.Unix(internal_BUILD_TIMESTAMP, 0)
}

// BuildNumber will return the build number for the application.
func BuildNumber() int64 {
	return internal_BUILD_NUMBER
}

// Version will return the version number for the application.
func Version() string {
	return internal_VERSION_STRING
}
