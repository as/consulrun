// +build !ent

package config

func DefaultEnterpriseSource() Source {
	return Source{
		Name:   "enterprise",
		Format: "hcl",
		Data:   ``,
	}
}
