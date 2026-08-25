package colfmt

import (
	"strconv"
	"strings"
)

// maxScale is the most fractional digits a decimal may carry. Coinbase quotes
// BTC-USD prices to 2 and sizes to 8; 18 is the most that can fit in an int64
// alongside any integer part at all, and anything beyond it takes the exception
// path rather than being rounded.
const maxScale = 18

// decimal is an exact decimal number: units × 10^-scale. It is what a price or
// a size is stored as — an integer and a digit count, never a float.
//
// The exchange sends decimal strings and this project stores what the exchange
// sent. A float64 cannot do that: it cannot represent 80691.53 exactly, and it
// cannot remember whether the string was "80691.5" or "80691.50". Both of those
// would show up as a price that is off by a hair or a size that lost a digit,
// which is the quiet kind of wrong this format is built to avoid.
type decimal struct {
	units int64
	scale uint8
}

// parseDecimal parses s exactly.
//
// ok is false when the value does not survive the round trip — too many digits
// for an int64, an exponent, a leading plus, a leading zero, "-0.0", anything.
// The test is not a list of rules but the round trip itself: the parse is
// accepted only if formatting the result reproduces s character for character.
// A value that fails is stored as its original string instead, so "unusual" and
// "corrupted" never become the same thing.
func parseDecimal(s string) (decimal, bool) {
	if s == "" {
		return decimal{}, false
	}
	body := s
	neg := false
	if body[0] == '-' {
		neg, body = true, body[1:]
	}
	intPart, fracPart := body, ""
	if i := strings.IndexByte(body, '.'); i >= 0 {
		intPart, fracPart = body[:i], body[i+1:]
	}
	if intPart == "" || len(fracPart) > maxScale {
		return decimal{}, false
	}
	digits := intPart + fracPart
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return decimal{}, false
		}
	}
	u, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return decimal{}, false
	}
	if neg {
		u = -u
	}
	d := decimal{units: u, scale: uint8(len(fracPart))}
	if d.String() != s {
		return decimal{}, false
	}
	return d, true
}

// String renders the decimal, padding the fraction to its scale so that a scale
// of 8 comes back with eight fractional digits however many of them are zero.
func (d decimal) String() string {
	s := strconv.FormatInt(d.units, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	if d.scale > 0 {
		for len(s) <= int(d.scale) {
			s = "0" + s
		}
		s = s[:len(s)-int(d.scale)] + "." + s[len(s)-int(d.scale):]
	}
	if neg {
		s = "-" + s
	}
	return s
}
