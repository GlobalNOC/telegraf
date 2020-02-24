package counter

import (
	"fmt"
	"math"
)

// Counter is a type used to calculate rates from measurement values
type Counter struct {
	Values map[string]*Measurement
}

// Measurement describes an individual measurement
type Measurement struct {
	Interval  int
	MinValue  float64
	MaxValue  float64
	Timestamp int64
	Value     float64
}

const (
	// DefaultMinValue is a good sane default for MinValue
	DefaultMinValue = 0

	// DefaultMaxValue is set to 1 Peta (e.g. 1 Pbps)
	DefaultMaxValue = 1000 * 1000 * 1000 * 1000 * 1000 // 1 Peta
)

// NewMeasurement constructs a new Measurement
func NewMeasurement(interval int, timestamp int64, min, max, value float64) *Measurement {
	return &Measurement{
		Interval:  interval,
		MinValue:  min,
		MaxValue:  max,
		Timestamp: timestamp,
		Value:     value,
	}
}

// String converts a Measurement into a string type for display
func (m *Measurement) String() string {
	return fmt.Sprintf("[interval: %d / minValue: %g / maxValue: %g / timestamp %d / value %g",
		m.Interval, m.MinValue, m.MaxValue, m.Timestamp, m.Value)
}

// NewCounter creates a new Counter
func NewCounter() *Counter {
	return &Counter{
		Values: make(map[string]*Measurement),
	}
}

// Add adds a Measurement to the Counter.
// Subsequent calls to Add will overwrite the old Measurement value
func (c *Counter) Add(name string, m *Measurement) {
	c.Values[name] = m
}

// GetRate calculates the rate of change between the given values and the last recorded values
// If the rate does not make sense, it returns an error
func (c *Counter) GetRate(name string, timestamp int64, value float64) (float64, error) {
	m, ok := c.Values[name]

	if !ok {
		return math.NaN(),
			fmt.Errorf("cannot update measurement for non-existent key %s", name)
	}

	latest := m.Value
	gap := timestamp - m.Timestamp

	m.Timestamp = timestamp
	m.Value = value

	if gap >= (int64(m.Interval) * 6) {
		return math.NaN(),
			fmt.Errorf("timestamp difference %d is too large; discarding update", gap)
	}

	if math.IsNaN(value) || math.IsNaN(latest) {
		return math.NaN(),
			fmt.Errorf("cannot do calculations on NaN values (%g, %g)", value, latest)
	}

	adjustedValue := value - latest

	if value < latest {
		if latest > float64(math.MaxUint32) {
			adjustedValue = float64(math.MaxUint64) - latest + value
		} else {
			adjustedValue = float64(math.MaxUint32) - latest + value
		}
	}

	rate := adjustedValue / float64(gap)

	if rate > m.MaxValue || rate < m.MinValue {
		return math.NaN(),
			fmt.Errorf("rate %g is outside the specified range [%g,%g]",
				rate, m.MinValue, m.MaxValue)
	}

	return rate, nil
}
