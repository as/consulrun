package version

import (
	"fmt"
	"strings"
)

var (
	GitCommit   string
	GitDescribe string

	//

	Version = "1.0.8"

	VersionPrerelease = "dev"
)

func GetHumanVersion() string {
	version := Version
	if GitDescribe != "" {
		version = GitDescribe
	}

	release := VersionPrerelease
	if GitDescribe == "" && release == "" {
		release = "dev"
	}
	if release != "" {
		version += fmt.Sprintf("-%s", release)
		if GitCommit != "" {
			version += fmt.Sprintf(" (%s)", GitCommit)
		}
	}

	return strings.Replace(version, "'", "", -1)
}
