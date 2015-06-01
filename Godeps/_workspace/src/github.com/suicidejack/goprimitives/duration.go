package goprimitives

import (
	"strings"
	"time"
)

// Duration with several helpful helpers
type Duration time.Duration

// NewDuration returns a new Duration instance
func NewDuration(duration string) (d *Duration, err error) {
	dur, err := readDuration(duration)
	tmp := Duration(dur)
	return &tmp, err
}

// MarshalJSON implements the JSON Marshaller interface
func (d *Duration) MarshalJSON() ([]byte, error) {
	if d == nil {
		return []byte("null"), nil
	}
	dur := time.Duration(*d)
	return []byte(`"` + dur.String() + `"`), nil
}

// UnmarshalJSON implements the JSON Unmarshaller interface.  Expects golang duration strings
// such as "1ms", "5s", etc.
func (d *Duration) UnmarshalJSON(b []byte) error {
	dur, err := readDuration(string(b))
	*d = Duration(dur)
	return err
}

// Set is used by the flag package to parse cli arguments and set the variable.
func (d *Duration) Set(value string) error {
	dur, err := readDuration(value)
	*d = Duration(dur)
	return err
}

// TimeDuration converts a Duration to a golang time.Duration
func (d Duration) TimeDuration() time.Duration {
	return time.Duration(d)
}

// IsPositive returns true if the duration is > 0ns
func (d Duration) IsPositive() bool {
	return time.Duration(d).Nanoseconds() > 0
}

func (d *Duration) String() string {
	if d == nil {
		return "<nil>"
	}
	return time.Duration(*d).String()
}

func readDuration(duration string) (d time.Duration, err error) {
	// some duration strings are quoted so this removes any quoting that may have occurred
	duration = strings.Replace(duration, `"`, "", -1)
	d, err = time.ParseDuration(duration)
	return
}
