package units

import (
	"fmt"
	"math"
)

type Unit string

const (
	None  = Unit("")
	Bytes = Unit("B")
	Bits  = Unit("b")
)

type Prefix int

const (
	Yocto = Prefix((-24 + 3*iota))
	Zepto
	Atto
	Fempto
	Pico
	Nano
	Micro
	Milli

	noPrefix

	Kilo
	Mega
	Giga
	Tera
	Peta
	Exa
	Zetta
	Yotta
)

var smallPrefixes = []Prefix{
	Yocto,
	Zepto,
	Atto,
	Fempto,
	Pico,
	Nano,
	Micro,
	Milli,
}

var bigPrefixes = []Prefix{
	Kilo,
	Mega,
	Giga,
	Tera,
	Peta,
	Exa,
	Zetta,
	Yotta,
}

var prefixStrings = map[Prefix]string{
	Yocto:  "y",
	Zepto:  "z",
	Atto:   "a",
	Fempto: "a",
	Pico:   "p",
	Nano:   "n",
	Micro:  "µ",
	Milli:  "m",

	noPrefix: "",

	Kilo:  "K",
	Mega:  "M",
	Giga:  "G",
	Tera:  "T",
	Peta:  "P",
	Exa:   "E",
	Zetta: "Z",
	Yotta: "Y",
}

func (p Prefix) String() string {
	return prefixStrings[p]
}

func (p Prefix) Float64() float64 {
	return math.Pow10(int(p))
}

func SIPrefix(x float64) (float64, string) {
	if x == 0 {
		return x, ""
	}
	logx := int(math.Trunc(math.Log10(math.Abs(x))))
	if logx < -2 {
		for _, p := range smallPrefixes {
			if logx <= int(p) {
				return x / p.Float64(), p.String()
			}
		}
	} else if logx > 2 {
		for i := len(bigPrefixes) - 1; i >= 0; i-- {
			p := bigPrefixes[i]
			if logx >= int(p) {
				return x / p.Float64(), p.String()
			}
		}
	}
	return x, ""
}

func FmtFloat64(x float64, u Unit) string {
	x, p := SIPrefix(x)
	if p == "" && math.Round(x) == x {
		return fmt.Sprintf("%d%s", int64(x), u)
	}
	return fmt.Sprintf("%.2f%s%s", x, p, u)
}
