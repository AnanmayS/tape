package colfmt

import "testing"

// TestParseDecimal pins down which strings take the scaled-integer path and
// which take the exception path. The rule is not a list: a parse is accepted
// only when re-rendering it reproduces the input exactly, so anything unusual
// is stored as itself rather than as an approximation of itself.
func TestParseDecimal(t *testing.T) {
	exact := map[string]decimal{
		"80691.53":   {units: 8069153, scale: 2},
		"0.00000001": {units: 1, scale: 8},
		"80691.50":   {units: 8069150, scale: 2},
		"1":          {units: 1, scale: 0},
		"0":          {units: 0, scale: 0},
		"1.000":      {units: 1000, scale: 3},
		"-1.5":       {units: -15, scale: 1},
		"0.0":        {units: 0, scale: 1},
	}
	for in, want := range exact {
		got, ok := parseDecimal(in)
		if !ok {
			t.Errorf("%q did not parse", in)
			continue
		}
		if got != want {
			t.Errorf("%q parsed as %+v, want %+v", in, got, want)
		}
		if got.String() != in {
			t.Errorf("%q rendered back as %q", in, got.String())
		}
	}

	// Everything here is a value this format stores as its own characters
	// instead: a leading zero, an exponent, a sign it would not re-emit, a
	// scale beyond an int64, or a number too large for one.
	odd := []string{
		"", "0000.5", "1e-8", "1E5", "+3", "-0.0", ".5", "1.", "nope", "1 ",
		"0.1234567890123456789", "12345678901234567890.1", "--1", "1.2.3",
	}
	for _, in := range odd {
		if d, ok := parseDecimal(in); ok {
			t.Errorf("%q parsed as %+v; it should have taken the exception path", in, d)
		}
	}
}

// TestDecimalStringPads checks the fraction is written to its full scale. A
// scale of 8 with one unit is 0.00000001, not 1e-8 and not 0.1.
func TestDecimalStringPads(t *testing.T) {
	for _, tc := range []struct {
		d    decimal
		want string
	}{
		{decimal{units: 1, scale: 8}, "0.00000001"},
		{decimal{units: -1, scale: 8}, "-0.00000001"},
		{decimal{units: 0, scale: 2}, "0.00"},
		{decimal{units: 100, scale: 2}, "1.00"},
		{decimal{units: -100, scale: 2}, "-1.00"},
		{decimal{units: 1 << 62, scale: 0}, "4611686018427387904"},
	} {
		if got := tc.d.String(); got != tc.want {
			t.Errorf("%+v rendered as %q, want %q", tc.d, got, tc.want)
		}
	}
}
