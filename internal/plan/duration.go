package plan

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from a YAML string such as "30s"
// or "1m30s". YAML has no native duration scalar, so plans spell durations the
// way Go and k6 both do.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return fmt.Errorf("duration must be a string like \"30s\": %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	if parsed < 0 {
		return fmt.Errorf("duration %q must not be negative", s)
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

func (d Duration) String() string { return time.Duration(d).String() }

func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d Duration) Proto() *durationpb.Duration { return durationpb.New(time.Duration(d)) }

func (d Duration) IsZero() bool { return time.Duration(d) == 0 }
