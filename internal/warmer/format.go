package warmer

import "strconv"

func trim(v float64) string { return strconv.FormatFloat(v, 'f', 4, 64) }
