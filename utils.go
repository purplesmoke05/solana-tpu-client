package solana_tpu_client

import (
	"sort"

	"golang.org/x/exp/constraints"
)

func contains(list []string, subset string) bool {
	for _, item := range list {
		if item == subset {
			return true
		}
	}
	return false
}

func sortSlice[T constraints.Ordered](s []T) {
	sort.Slice(s, func(i, j int) bool {
		return s[i] < s[j]
	})
}

func reverseSlice[T constraints.Ordered](s []T) {
	sort.Slice(s, func(i, j int) bool {
		return s[i] > s[j]
	})
}
