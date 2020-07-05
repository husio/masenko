package proto

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type SDuration time.Duration

func (d SDuration) Duration() time.Duration {
	return time.Duration(d)
}

func (d *SDuration) UnmarshalJSON(raw []byte) error {
	var dur time.Duration
	if err := json.Unmarshal(raw, &dur); err == nil {
		*d = SDuration(dur)
		return nil
	}

	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return errors.New("invalid duration format")
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration string: %s", err)
	}
	*d = SDuration(dur)
	return nil
}
