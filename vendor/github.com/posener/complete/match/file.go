package match

import "strings"

func File(file, prefix string) bool {

	if file == "./" && (prefix == "." || prefix == "") {
		return true
	}
	if prefix == "." && strings.HasPrefix(file, ".") {
		return true
	}

	file = strings.TrimPrefix(file, "./")
	prefix = strings.TrimPrefix(prefix, "./")

	return strings.HasPrefix(file, prefix)
}
