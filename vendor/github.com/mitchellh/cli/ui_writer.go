package cli

type UiWriter struct {
	Ui Ui
}

func (w *UiWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	if n > 0 && p[n-1] == '\n' {
		p = p[:n-1]
	}

	w.Ui.Info(string(p))
	return n, nil
}
