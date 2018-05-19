package copystructure

import (
	"reflect"
	"time"
)

func init() {
	Copiers[reflect.TypeOf(time.Time{})] = timeCopier
}

func timeCopier(v interface{}) (interface{}, error) {

	return v.(time.Time), nil
}
