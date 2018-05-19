package complete

type Command struct {
	Sub Commands

	Flags Flags

	GlobalFlags Flags

	Args Predictor
}

func (c *Command) Predict(a Args) []string {
	options, _ := c.predict(a)
	return options
}

type Commands map[string]Command

func (c Commands) Predict(a Args) (prediction []string) {
	for sub := range c {
		prediction = append(prediction, sub)
	}
	return
}

type Flags map[string]Predictor

func (f Flags) Predict(a Args) (prediction []string) {
	for flag := range f {

		flagHyphenStart := len(flag) != 0 && flag[0] == '-'
		lastHyphenStart := len(a.Last) != 0 && a.Last[0] == '-'
		if flagHyphenStart && !lastHyphenStart {
			continue
		}
		prediction = append(prediction, flag)
	}
	return
}

func (c *Command) predict(a Args) (options []string, only bool) {

	subCommandFound := false
	for i, arg := range a.Completed {
		if cmd, ok := c.Sub[arg]; ok {
			subCommandFound = true

			options, only = cmd.predict(a.from(i))
			if only {
				return
			}

			break
		}
	}

	if predictor, ok := c.GlobalFlags[a.LastCompleted]; ok && predictor != nil {
		Log("Predicting according to global flag %s", a.LastCompleted)
		return predictor.Predict(a), true
	}

	options = append(options, c.GlobalFlags.Predict(a)...)

	if subCommandFound {
		return
	}

	if predictor, ok := c.Flags[a.LastCompleted]; ok && predictor != nil {
		Log("Predicting according to flag %s", a.LastCompleted)
		return predictor.Predict(a), true
	}

	options = append(options, c.Sub.Predict(a)...)
	options = append(options, c.Flags.Predict(a)...)
	if c.Args != nil {
		options = append(options, c.Args.Predict(a)...)
	}

	return
}
