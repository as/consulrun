package gophercloud

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

func WaitFor(timeout int, predicate func() (bool, error)) error {
	type WaitForResult struct {
		Success bool
		Error   error
	}

	start := time.Now().Unix()

	for {

		if timeout >= 0 && time.Now().Unix()-start >= int64(timeout) {
			return fmt.Errorf("A timeout occurred")
		}

		time.Sleep(1 * time.Second)

		var result WaitForResult
		ch := make(chan bool, 1)
		go func() {
			defer close(ch)
			satisfied, err := predicate()
			result.Success = satisfied
			result.Error = err
		}()

		select {
		case <-ch:
			if result.Error != nil {
				return result.Error
			}
			if result.Success {
				return nil
			}

		case <-time.After(time.Duration(timeout) * time.Second):
			return fmt.Errorf("A timeout occurred")
		}
	}
}

//
func NormalizeURL(url string) string {
	if !strings.HasSuffix(url, "/") {
		return url + "/"
	}
	return url
}

func NormalizePathURL(basePath, rawPath string) (string, error) {
	u, err := url.Parse(rawPath)
	if err != nil {
		return "", err
	}

	if u.Scheme != "" {
		return u.String(), nil
	}

	bu, err := url.Parse(basePath)
	if err != nil {
		return "", err
	}
	var basePathSys, absPathSys string
	if bu.Scheme != "" {
		basePathSys = filepath.FromSlash(bu.Path)
		absPathSys = filepath.Join(basePathSys, rawPath)
		bu.Path = filepath.ToSlash(absPathSys)
		return bu.String(), nil
	}

	absPathSys = filepath.Join(basePath, rawPath)
	u.Path = filepath.ToSlash(absPathSys)
	if err != nil {
		return "", err
	}
	u.Scheme = "file"
	return u.String(), nil

}
