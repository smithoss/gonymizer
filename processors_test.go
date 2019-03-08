package gonymizer

import (
	"github.com/google/uuid"
	"testing"

	"github.com/stretchr/testify/assert"
)

var proc = []ProcessorDefinition{
	{
		Name:     "",
		Max:      0,
		Min:      0,
		Variance: 0,
		Comment:  "",
	},
}

var cMap = ColumnMapper{
	TableSchema:     "",
	TableName:       "",
	ColumnName:      "",
	DataType:        "",
	ParentSchema:    "",
	ParentTable:     "",
	ParentColumn:    "",
	OrdinalPosition: 4,
	IsNullable:      false,
	Processors:      proc,
	Comment:         "",
}

func TestProcessorFunc(t *testing.T) {
}

func TestProcessorAlphaNumericScrambler(t *testing.T) {
	var alphaTest ColumnMapper

	output, err := ProcessorAlphaNumericScrambler(&cMap, "AsDfG10*&")
	assert.Nil(t, err)
	assert.NotEqual(t, output, "AsDfG0*&")

	alphaTest.ParentSchema = "test_schema"
	alphaTest.ParentTable = "test_table"
	alphaTest.ParentColumn = "test_column"

	outputA, err := ProcessorAlphaNumericScrambler(&alphaTest, "My name is Mud - Pr1mUs")
	assert.Nil(t, err)
	outputB, err := ProcessorAlphaNumericScrambler(&alphaTest, "My name is Mud - 111111")
	assert.Nil(t, err)
	outputC, err := ProcessorAlphaNumericScrambler(&alphaTest, "My name is Mud - Pr1mUs")
	assert.Nil(t, err)

	// outputA != outputB
	assert.NotEqual(t, outputA, outputB)
	// outputA === outputC
	assert.Equal(t, outputA, outputC)
}

func TestProcessorAddress(t *testing.T) {
	output, err := ProcessorAddress(&cMap, "1234 Testing Lane")
	assert.Nil(t, err)
	assert.NotEqual(t, output, "1234 Testing Lane")

	// Jaro-Winkler distance will be 0 for empty string. Should get an error here.
	output, err = ProcessorAddress(&cMap, "")
	assert.NotNil(t, err)
	assert.NotEqual(t, output, "")
}

func TestProcessorCity(t *testing.T) {
	output, err := ProcessorCity(&cMap, "Rick and Morty Ville")
	assert.Nil(t, err)
	assert.NotEqual(t, output, "Rick and Morty Ville")

	// Jaro-Winkler distance will be 0 for empty string. Should get an error here.
	output, err = ProcessorCity(&cMap, "")
	assert.NotNil(t, err)
	assert.NotEqual(t, output, "")
}

func TestProcessorEmailAddress(t *testing.T) {
	output, err := ProcessorEmailAddress(&cMap, "rick@morty.example.com")
	assert.Nil(t, err)
	assert.NotEqual(t, output, "rick@morty.example.com")

	// Jaro-Winkler distance will be 0 for empty string. Should get an error here.
	output, err = ProcessorEmailAddress(&cMap, "")
	assert.NotNil(t, err)
	assert.NotEqual(t, output, "")
}

func TestProcessorFirstName(t *testing.T) {
	output, err := ProcessorFirstName(&cMap, "RickMortyRick")
	assert.Nil(t, err)
	assert.NotEqual(t, output, "RickMortyRick")

	// Jaro-Winkler distance will be 0 for empty string. Should get an error here.
	output, err = ProcessorFirstName(&cMap, "")
	assert.NotNil(t, err)
	assert.NotEqual(t, output, "")
}

func TestProcessorFullName(t *testing.T) {
	output, err := ProcessorFullName(&cMap, "Morty & Rick")
	assert.Nil(t, err)
	assert.NotEqual(t, output, "Morty & Rick")

	// Jaro-Winkler distance will be 0 for empty string. Should get an error here.
	output, err = ProcessorFullName(&cMap, "")
	assert.NotNil(t, err)
	assert.NotEqual(t, output, "")
}

func TestProcessorIdentity(t *testing.T) {
	output, err := ProcessorIdentity(&cMap, "Hi Rick!")
	assert.Nil(t, err)
	assert.Equal(t, output, "Hi Rick!")

	output, err = ProcessorIdentity(&cMap, "")
	assert.Nil(t, err)
	assert.Equal(t, output, "")
}

func TestProcessorLastName(t *testing.T) {
	output, err := ProcessorLastName(&cMap, "Bye Rick!")
	assert.Nil(t, err)
	assert.NotEqual(t, output, "Bye Rick!")

	// Jaro-Winkler distance will be 0 for empty string. Should get an error here.
	output, err = ProcessorLastName(&cMap, "")
	assert.NotNil(t, err)
	assert.NotEqual(t, output, "")
}

func TestProcessorPhoneNumber(t *testing.T) {
	output, err := ProcessorPhoneNumber(&cMap, "+18885551212")
	assert.Nil(t, err)
	assert.NotEqual(t, output, "+18885551212")

	// Jaro-Winkler distance will be 0 for empty string. Should get an error here.
	output, err = ProcessorPhoneNumber(&cMap, "")
	assert.NotNil(t, err)
	assert.NotEqual(t, output, "")
}

func TestProcessorState(t *testing.T) {
	output, err := ProcessorState(&cMap, "Antarctica")
	assert.Nil(t, err)
	assert.NotEqual(t, output, "Antarctica")

	// Jaro-Winkler distance will be 0 for empty string. Should get an error here.
	output, err = ProcessorState(&cMap, "")
	assert.NotNil(t, err)
	assert.NotEqual(t, output, "")
}

func TestProcessorUserName(t *testing.T) {
	output, err := ProcessorUserName(&cMap, "Ricky and Julian")
	assert.Nil(t, err)
	assert.NotEqual(t, output, "Ricky and Julian")

	// Jaro-Winkler distance will be 0 for empty string. Should get an error here.
	output, err = ProcessorUserName(&cMap, "")
	assert.NotNil(t, err)
	assert.NotEqual(t, output, "")
}

func TestProcessorZip(t *testing.T) {
	output, err := ProcessorZip(&cMap, "00000-00")
	assert.Nil(t, err)
	assert.NotEqual(t, output, "00000-00")

	// Jaro-Winkler distance will be 0 for empty string. Should get an error here.
	output, err = ProcessorZip(&cMap, "")
	assert.NotNil(t, err)
	assert.NotEqual(t, output, "")
}

func TestProcessorRandomDate(t *testing.T) {
	var failBoats = []string{
		"I AM THE FAIL BOAT!",
		"01.01.1970",
		"01/01/1970",
		"1970.01.01",
		"1970/01/01",
	}

	output, err := ProcessorRandomDate(&cMap, "1970-01-01")
	assert.Nil(t, err)
	assert.NotEqual(t, output, "1970-01-01")

	for _, tst := range failBoats {
		output, err = ProcessorRandomDate(&cMap, tst)
		assert.NotNil(t, err)
		assert.NotEqual(t, output, nil)
	}
}

func TestProcessorRandomUUID(t *testing.T) {
	var testUUID uuid.UUID

	testUUID, err := uuid.NewUUID()
	assert.Nil(t, err)

	output, err := ProcessorRandomUUID(&cMap, testUUID.String())
	assert.Nil(t, err)
	assert.NotEqual(t, output, testUUID)

	if val, found := UUIDMap[testUUID]; found {
		if val == testUUID {
			t.Fatalf("UUIDs match\t%s <=> %s", testUUID.String(), val.String())
		}
	} else {
		t.Fatalf("Unable to find UUID '%s' in the UUID map!", output)
	}
	output, err = ProcessorRandomUUID(&cMap, "")
	assert.NotNil(t, err)
	assert.Equal(t, output, "")
}

func TestProcessorScrubString(t *testing.T) {
	output, err := ProcessorScrubString(&cMap, "Ricky and Julian")
	assert.Nil(t, err)
	assert.NotEqual(t, output, "Ricky and Julian")

	output, err = ProcessorScrubString(&cMap, "")
	assert.Nil(t, err)
	assert.Equal(t, output, "")
}

func TestRandomizeUUID(t *testing.T) {
	tempUUID := uuid.New().String()
	output, err := ProcessorRandomUUID(&cMap, tempUUID)
	assert.Nil(t, err)
	assert.NotEqual(t, output, tempUUID)

	output, err = ProcessorRandomUUID(&cMap, "")
	assert.NotNil(t, err)
	assert.Equal(t, output, "")
}
