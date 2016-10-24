package swift_test

import (
	"restic"

	"restic/errors"

	"restic/backend/swift"
	"restic/backend/test"
	. "restic/test"
)

//go:generate go run ../test/generate_backend_tests.go

func init() {
	if TestSwiftServer == "" {
		SkipMessage = "swift test server not available"
		return
	}

	cfg := swift.Config{
		Container:  "restictestcontainer",
		StorageURL: TestSwiftServer,
		AuthToken:  TestSwiftToken,
	}

	test.CreateFn = func() (restic.Backend, error) {
		be, err := swift.Open(cfg)
		if err != nil {
			return nil, err
		}

		exists, err := be.Test(restic.ConfigFile, "")
		if err != nil {
			return nil, err
		}

		if exists {
			return nil, errors.New("config already exists")
		}

		return be, nil
	}

	test.OpenFn = func() (restic.Backend, error) {
		return swift.Open(cfg)
	}

	// test.CleanupFn = func() error {
	// 	if tempBackendDir == "" {
	// 		return nil
	// 	}

	// 	fmt.Printf("removing test backend at %v\n", tempBackendDir)
	// 	err := os.RemoveAll(tempBackendDir)
	// 	tempBackendDir = ""
	// 	return err
	// }
}
