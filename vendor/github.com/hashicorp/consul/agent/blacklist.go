package agent

import (
	"github.com/armon/go-radix"
)

type Blacklist struct {
	tree *radix.Tree
}

func NewBlacklist(prefixes []string) *Blacklist {
	tree := radix.New()
	for _, prefix := range prefixes {
		tree.Insert(prefix, nil)
	}
	return &Blacklist{tree}
}

func (b *Blacklist) Block(path string) bool {
	_, _, blocked := b.tree.LongestPrefix(path)
	return blocked
}
