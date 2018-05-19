package complete

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/posener/complete/match"
)

func PredictDirs(pattern string) Predictor {
	return files(pattern, false)
}

func PredictFiles(pattern string) Predictor {
	return files(pattern, true)
}

func files(pattern string, allowFiles bool) PredictFunc {

	return func(a Args) (prediction []string) {
		prediction = predictFiles(a, pattern, allowFiles)

		if len(prediction) != 1 {
			return
		}

		if stat, err := os.Stat(prediction[0]); err != nil || !stat.IsDir() {
			return
		}

		a.Last = prediction[0]
		return predictFiles(a, pattern, allowFiles)
	}
}

func predictFiles(a Args, pattern string, allowFiles bool) []string {
	if strings.HasSuffix(a.Last, "/..") {
		return nil
	}

	dir := a.Directory()
	files := listFiles(dir, pattern, allowFiles)

	files = append(files, dir)

	return PredictFilesSet(files).Predict(a)
}

func PredictFilesSet(files []string) PredictFunc {
	return func(a Args) (prediction []string) {

		for _, f := range files {
			f = fixPathForm(a.Last, f)

			if match.File(f, a.Last) {
				prediction = append(prediction, f)
			}
		}
		return
	}
}

func listFiles(dir, pattern string, allowFiles bool) []string {

	m := map[string]bool{}

	if files, err := filepath.Glob(filepath.Join(dir, pattern)); err == nil {
		for _, f := range files {
			if stat, err := os.Stat(f); err != nil || stat.IsDir() || allowFiles {
				m[f] = true
			}
		}
	}

	if dirs, err := ioutil.ReadDir(dir); err == nil {
		for _, d := range dirs {
			if d.IsDir() {
				m[filepath.Join(dir, d.Name())] = true
			}
		}
	}

	list := make([]string, 0, len(m))
	for k := range m {
		list = append(list, k)
	}
	return list
}
