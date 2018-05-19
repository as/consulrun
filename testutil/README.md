# Testutil

	Just like original, except adds a NewQuietServer() initializer.
	
```
import (
	"github.com/hashicorp/consul/api" // uses the real consul client
)
func example(){
	s, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	c, err := api.NewClient(&api.Config{Address: s.HTTPAddr})
	
	// ... do things with it
}

func example2(){
	s, err := testutil.NewQuietServer()
	if err != nil {
		t.Fatal(err)
	}
	c, err := api.NewClient(&api.Config{Address: s.HTTPAddr})
	
	// ... do things with it (and it wont flood stdout and stderr)
}
```