package complete

type Predictor interface {
	Predict(Args) []string
}

func PredictOr(predictors ...Predictor) Predictor {
	return PredictFunc(func(a Args) (prediction []string) {
		for _, p := range predictors {
			if p == nil {
				continue
			}
			prediction = append(prediction, p.Predict(a)...)
		}
		return
	})
}

type PredictFunc func(Args) []string

func (p PredictFunc) Predict(a Args) []string {
	if p == nil {
		return nil
	}
	return p(a)
}

var PredictNothing Predictor

var PredictAnything = PredictFunc(func(Args) []string { return nil })
