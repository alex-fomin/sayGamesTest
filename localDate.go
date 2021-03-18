package main

import (
	"fmt"
	"strings"
	"time"
)

type LocalDateTime struct {
	time.Time
}

const layout = "2006-01-02 15:04:05"

func (l *LocalDateTime) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), `"`) // remove quotes
	if s == "null" {
		return
	}
	l.Time, err = time.Parse(layout, s)
	return
}

func (l LocalDateTime) MarshalJSON() ([]byte, error) {
	if l.Time.IsZero() {
		return nil, nil
	}
	return []byte(fmt.Sprintf(`"%s"`, l.Time.Format(layout))), nil
}
