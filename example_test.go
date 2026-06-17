package portbeam_test

import (
	"fmt"

	"github.com/sandrino/portbeam"
)

func ExampleParseSpecs() {
	specs, err := portbeam.ParseSpecs([]string{
		"127.0.0.1:18080=10.0.0.5:8080",
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(specs[0].Listen)
	fmt.Println(specs[0].Target)

	// Output:
	// 127.0.0.1:18080
	// 10.0.0.5:8080
}
