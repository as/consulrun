package metadata

import (
	"github.com/as/consulrun/hashicorp/go-version"
	"github.com/as/consulrun/hashicorp/serf/serf"
)

func Build(m *serf.Member) (*version.Version, error) {
	str := versionFormat.FindString(m.Tags["build"])
	return version.NewVersion(str)
}
