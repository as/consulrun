package metadata

import (
	"github.com/hashicorp/go-version"
	"github.com/hashicorp/serf/serf"
)

func Build(m *serf.Member) (*version.Version, error) {
	str := versionFormat.FindString(m.Tags["build"])
	return version.NewVersion(str)
}
