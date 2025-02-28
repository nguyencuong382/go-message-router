package mrouter

import "strings"

func MergeKeys(keys ...string) string {
	return strings.Join(keys, "-")
}
