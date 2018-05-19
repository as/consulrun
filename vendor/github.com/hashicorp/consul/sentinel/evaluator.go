package sentinel

type Evaluator interface {
	Compile(policy string) error
	Execute(policy string, enforcementLevel string, data map[string]interface{}) bool
}
