package gonymizer

import (
	"reflect"
	"testing"
)

type Checker struct {
	Label     string
	Expected  interface{}
	Candidate interface{}
}

func CheckAll(t *testing.T, checks []Checker) {
	for i, fc := range checks {

		if reflect.TypeOf(fc.Candidate) != reflect.TypeOf(fc.Expected) {

			t.Errorf("%d, %s mismatched type:\nexpected %T\n    got %T\n",
				i,
				fc.Label,
				fc.Expected,
				fc.Candidate,
			)
		}

		if reflect.DeepEqual(fc.Expected, fc.Candidate) == false {
			t.Errorf("%d, %s:\nexpected\n%+v\n     got\n%+v\n", i, fc.Label, fc.Expected, fc.Candidate)
			if reflect.TypeOf(fc.Candidate) == reflect.TypeOf(map[string]interface{}{}) {
				can := fc.Candidate.(map[string]interface{})
				for k, v := range can {
					t.Logf("candidate kv %s: (%T) %+v", k, v, v)
				}
			}
		}
	}
}

/*
func VerifyFileSize(t *testing.T, filePath string, size int64) error {
	fp, err := os.OpenFile(filePath, os.O_RDONLY, 0660)
	assert.Nil(t, err)
	defer fp.Close()

	fpd, err := fp.Stat()
	assert.Nil(t, err)

	if fpd.Size() != size {
		errMsg := fmt.Sprintf("Unable to save test file %s => File size of %d bytes != %d bytes",
			filePath,
			fpd.Size(),
			size,
		)
		err = errors.New(errMsg)
	}
	assert.Nil(t, fp.Close())
	return err
}
*/
