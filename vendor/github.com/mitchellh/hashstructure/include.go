package hashstructure

type Includable interface {
	HashInclude(field string, v interface{}) (bool, error)
}

type IncludableMap interface {
	HashIncludeMap(field string, k, v interface{}) (bool, error)
}
