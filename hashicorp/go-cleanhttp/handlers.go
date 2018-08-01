package cleanhttp

import (
	"net/http"
	"strings"
	"unicode"
)

type HandlerInput struct {
	ErrStatus int
}

func PrintablePathCheckHandler(next http.Handler, input *HandlerInput) http.Handler {

	if input == nil {
		input = &HandlerInput{
			ErrStatus: http.StatusBadRequest,
		}
	}

	if input.ErrStatus == 0 {
		input.ErrStatus = http.StatusBadRequest
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		idx := strings.IndexFunc(r.URL.Path, func(c rune) bool {
			return !unicode.IsPrint(c)
		})

		if idx != -1 {
			w.WriteHeader(input.ErrStatus)
			return
		}

		next.ServeHTTP(w, r)
		return
	})
}
