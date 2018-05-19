package match

import "strings"

func Prefix(long, prefix string) bool {
	return strings.HasPrefix(long, prefix)
}
