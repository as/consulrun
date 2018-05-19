package speakeasy

import (
	"fmt"
	"io"
	"os"
	"strings"
)

func Ask(prompt string) (password string, err error) {
	return FAsk(os.Stdout, prompt)
}

func FAsk(wr io.Writer, prompt string) (password string, err error) {
	if prompt != "" {
		fmt.Fprint(wr, prompt) // Display the prompt.
	}
	password, err = getPassword()

	fmt.Fprintln(wr, "")
	return
}

func readline() (value string, err error) {
	var valb []byte
	var n int
	b := make([]byte, 1)
	for {

		n, err = os.Stdin.Read(b)
		if err != nil && err != io.EOF {
			return "", err
		}
		if n == 0 || b[0] == '\n' {
			break
		}
		valb = append(valb, b[0])
	}

	return strings.TrimSuffix(string(valb), "\r"), nil
}
