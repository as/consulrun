package acl

import (
	"crypto/md5"
	"fmt"

	"github.com/hashicorp/consul/sentinel"
	"github.com/hashicorp/golang-lru"
)

type FaultFunc func(id string) (string, string, error)

type aclEntry struct {
	ACL    ACL
	Parent string
	RuleID string
}

type Cache struct {
	faultfn     FaultFunc
	aclCache    *lru.TwoQueueCache // Cache id -> acl
	policyCache *lru.TwoQueueCache // Cache policy -> acl
	ruleCache   *lru.TwoQueueCache // Cache rules -> policy
	sentinel    sentinel.Evaluator
}

func NewCache(size int, faultfn FaultFunc, sentinel sentinel.Evaluator) (*Cache, error) {
	if size <= 0 {
		return nil, fmt.Errorf("Must provide positive cache size")
	}

	rc, err := lru.New2Q(size)
	if err != nil {
		return nil, err
	}

	pc, err := lru.New2Q(size)
	if err != nil {
		return nil, err
	}

	ac, err := lru.New2Q(size)
	if err != nil {
		return nil, err
	}

	c := &Cache{
		faultfn:     faultfn,
		aclCache:    ac,
		policyCache: pc,
		ruleCache:   rc,
		sentinel:    sentinel,
	}
	return c, nil
}

func (c *Cache) GetPolicy(rules string) (*Policy, error) {
	return c.getPolicy(RuleID(rules), rules)
}

func (c *Cache) getPolicy(id, rules string) (*Policy, error) {
	raw, ok := c.ruleCache.Get(id)
	if ok {
		return raw.(*Policy), nil
	}
	policy, err := Parse(rules, c.sentinel)
	if err != nil {
		return nil, err
	}
	policy.ID = id
	c.ruleCache.Add(id, policy)
	return policy, nil

}

func RuleID(rules string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(rules)))
}

func (c *Cache) policyID(parent, ruleID string) string {
	return parent + ":" + ruleID
}

func (c *Cache) GetACLPolicy(id string) (string, *Policy, error) {

	if raw, ok := c.aclCache.Get(id); ok {
		cached := raw.(aclEntry)
		if raw, ok := c.ruleCache.Get(cached.RuleID); ok {
			return cached.Parent, raw.(*Policy), nil
		}
	}

	parent, rules, err := c.faultfn(id)
	if err != nil {
		return "", nil, err
	}

	policy, err := c.GetPolicy(rules)
	return parent, policy, err
}

func (c *Cache) GetACL(id string) (ACL, error) {

	raw, ok := c.aclCache.Get(id)
	if ok {
		return raw.(aclEntry).ACL, nil
	}

	parentID, rules, err := c.faultfn(id)
	if err != nil {
		return nil, err
	}
	ruleID := RuleID(rules)

	policyID := c.policyID(parentID, ruleID)
	var compiled ACL
	if raw, ok := c.policyCache.Get(policyID); ok {
		compiled = raw.(ACL)
	} else {

		policy, err := c.getPolicy(ruleID, rules)
		if err != nil {
			return nil, err
		}

		parent := RootACL(parentID)
		if parent == nil {
			parent, err = c.GetACL(parentID)
			if err != nil {
				return nil, err
			}
		}

		acl, err := New(parent, policy, c.sentinel)
		if err != nil {
			return nil, err
		}

		c.policyCache.Add(policyID, acl)
		compiled = acl
	}

	c.aclCache.Add(id, aclEntry{compiled, parentID, ruleID})
	return compiled, nil
}

func (c *Cache) ClearACL(id string) {
	c.aclCache.Remove(id)
}

func (c *Cache) Purge() {
	c.aclCache.Purge()
}
