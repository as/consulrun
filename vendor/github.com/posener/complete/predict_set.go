package complete

func PredictSet(options ...string) Predictor {
	return predictSet(options)
}

type predictSet []string

func (p predictSet) Predict(a Args) []string {
	return p
}
