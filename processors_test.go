package gonymizer

import (
	"regexp"
	"testing"

	"github.com/google/uuid"

	"github.com/stretchr/testify/require"
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
	require.Nil(t, err)
	require.NotEqual(t, output, "AsDfG0*&")

	alphaTest.ParentSchema = "test_schema"
	alphaTest.ParentTable = "test_table"
	alphaTest.ParentColumn = "test_column"

	outputA, err := ProcessorAlphaNumericScrambler(&alphaTest, "My name is Mud - Pr1mUs")
	require.Nil(t, err)
	outputB, err := ProcessorAlphaNumericScrambler(&alphaTest, "My name is Mud - 111111")
	require.Nil(t, err)
	outputC, err := ProcessorAlphaNumericScrambler(&alphaTest, "My name is Mud - Pr1mUs")
	require.Nil(t, err)

	// outputA != outputB
	require.NotEqual(t, outputA, outputB)
	// outputA === outputC
	require.Equal(t, outputA, outputC)

	const escapeSequences = "\\\\\\t\\n\\f\\b\\1\\21\\337\\x1\\xF2\\u4AE1\\UDEADBEEF"
	outputEscapes, err := ProcessorAlphaNumericScrambler(&alphaTest, escapeSequences)
	require.Nil(t, err)
	require.Equal(t, outputEscapes, escapeSequences)
}

func TestProcessorAddress(t *testing.T) {
	output, err := ProcessorAddress(&cMap, "1234 Testing Lane")
	require.Nil(t, err)
	require.NotEqual(t, output, "1234 Testing Lane")

	output, err = ProcessorAddress(&cMap, "")
	require.Nil(t, err)
	require.NotEqual(t, output, "")
}

func TestProcessorCity(t *testing.T) {
	output, err := ProcessorCity(&cMap, "Rick and Morty Ville")
	require.Nil(t, err)
	require.NotEqual(t, output, "Rick and Morty Ville")

	output, err = ProcessorCity(&cMap, "")
	require.Nil(t, err)
	require.NotEqual(t, output, "")
}

func TestProcessorEmailAddress(t *testing.T) {
	output, err := ProcessorEmailAddress(&cMap, "rick@morty.example.com")
	require.Nil(t, err)
	require.NotEqual(t, output, "rick@morty.example.com")

	output, err = ProcessorEmailAddress(&cMap, "")
	require.Nil(t, err)
	require.NotEqual(t, output, "")
}

func TestProcessorFirstName(t *testing.T) {
	output, err := ProcessorFirstName(&cMap, "RickMortyRick")
	require.Nil(t, err)
	require.NotEqual(t, output, "RickMortyRick")

	output, err = ProcessorFirstName(&cMap, "")
	require.Nil(t, err)
	require.NotEqual(t, output, "")
}

func TestProcessorFullName(t *testing.T) {
	output, err := ProcessorFullName(&cMap, "Morty & Rick")
	require.Nil(t, err)
	require.NotEqual(t, output, "Morty & Rick")

	output, err = ProcessorFullName(&cMap, "")
	require.Nil(t, err)
	require.NotEqual(t, output, "")
}

func TestProcessorIdentity(t *testing.T) {
	output, err := ProcessorIdentity(&cMap, "Hi Rick!")
	require.Nil(t, err)
	require.Equal(t, output, "Hi Rick!")

	output, err = ProcessorIdentity(&cMap, "")
	require.Nil(t, err)
	require.Equal(t, output, "")
}

func TestProcessorIPv4(t *testing.T) {
	output, err := ProcessorIPv4(&cMap, "127.0.0.1")
	require.Nil(t, err)
	require.NotEqual(t, output, "127.0.0.1")
	re, _ := regexp.Compile(`(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}`)
	require.True(t, re.MatchString(output))

	output, err = ProcessorIPv4(&cMap, "")
	require.Nil(t, err)
	require.NotEqual(t, output, "")
	require.True(t, re.MatchString(output))
}

func TestProcessorLastName(t *testing.T) {
	output, err := ProcessorLastName(&cMap, "Bye Rick!")
	require.Nil(t, err)
	require.NotEqual(t, output, "Bye Rick!")

	output, err = ProcessorLastName(&cMap, "")
	require.Nil(t, err)
	require.NotEqual(t, output, "")
}

func TestProcessorPhoneNumber(t *testing.T) {
	output, err := ProcessorPhoneNumber(&cMap, "+18885551212")
	require.Nil(t, err)
	require.NotEqual(t, output, "+18885551212")

	output, err = ProcessorPhoneNumber(&cMap, "")
	require.Nil(t, err)
	require.NotEqual(t, output, "")
}

func TestProcessorState(t *testing.T) {
	output, err := ProcessorState(&cMap, "Antarctica")
	require.Nil(t, err)
	require.NotEqual(t, output, "Antarctica")

	output, err = ProcessorState(&cMap, "")
	require.Nil(t, err)
	require.NotEqual(t, output, "")
}

func TestProcessorStateAbbrev(t *testing.T) {
	output, err := ProcessorStateAbbrev(&cMap, "NY")
	require.Nil(t, err)
	require.NotEqual(t, output, "NY")

	output, err = ProcessorStateAbbrev(&cMap, "")
	require.Nil(t, err)
	require.NotEqual(t, output, "")
	require.Len(t, output, 2)
}

func TestProcessorUserName(t *testing.T) {
	output, err := ProcessorUserName(&cMap, "Ricky and Julian")
	require.Nil(t, err)
	require.NotEqual(t, output, "Ricky and Julian")

	output, err = ProcessorUserName(&cMap, "")
	require.Nil(t, err)
	require.NotEqual(t, output, "")
}

func TestProcessorZip(t *testing.T) {
	output, err := ProcessorZip(&cMap, "00000-00")
	require.Nil(t, err)
	require.NotEqual(t, output, "00000-00")

	output, err = ProcessorZip(&cMap, "")
	require.Nil(t, err)
	require.NotEqual(t, output, "")
}

func TestProcessorCompanyName(t *testing.T) {
	output, err := ProcessorCompanyName(&cMap, "RickMortyRick")
	require.Nil(t, err)
	require.NotEqual(t, output, "RickMortyRick")

	output, err = ProcessorCompanyName(&cMap, "")
	require.Nil(t, err)
	require.NotEqual(t, output, "")
}

func TestProcessorRandomBoolean(t *testing.T) {
	output, err := ProcessorRandomBoolean(&cMap, "FALSE")
	require.Nil(t, err)
	require.Contains(t, []string{"TRUE", "FALSE"}, output)
}

func TestProcessorRandomDate(t *testing.T) {
	var failBoats = []string{
		"I AM THE FAIL BOAT!",
		"01.01.1970",
		"01/01/1970",
		"1970.01.01",
		"1970/01/01",
		"",
	}

	output, err := ProcessorRandomDate(&cMap, "1970-01-01")
	require.Nil(t, err)
	require.NotEqual(t, output, "1970-01-01")

	for _, tst := range failBoats {
		output, err = ProcessorRandomDate(&cMap, tst)
		require.NotNil(t, err)
		require.NotEqual(t, output, nil)
	}
}

func TestProcessorRandomDigits(t *testing.T) {
	digits := []string{"12345", "123456", "1234567", "12345678"}
	for _, d := range digits {
		output, err := ProcessorRandomDigits(&cMap, d)
		require.Nil(t, err)
		require.NotEqual(t, d, output)
		require.Equal(t, len(d), len(output))
	}

	output, err := ProcessorRandomDigits(&cMap, "")
	require.Nil(t, err)
	require.Equal(t, "", output)
}

func TestProcessorRandomUUID(t *testing.T) {
	var testUUID uuid.UUID

	testUUID, err := uuid.NewUUID()
	require.Nil(t, err)

	output, err := ProcessorRandomUUID(&cMap, testUUID.String())
	require.Nil(t, err)
	require.NotEqual(t, output, testUUID)

	val, _ := UUIDMap.Get(testUUID)
	if val == testUUID.String() {
		t.Fatalf("UUIDs match\t%s <=> %s", testUUID.String(), val)
	}
	output, err = ProcessorRandomUUID(&cMap, "")
	require.NotNil(t, err)
	require.Equal(t, output, "")
}

func TestProcessorScrubString(t *testing.T) {
	output, err := ProcessorScrubString(&cMap, "Ricky and Julian")
	require.Nil(t, err)
	require.NotEqual(t, output, "Ricky and Julian")

	output, err = ProcessorScrubString(&cMap, "")
	require.Nil(t, err)
	require.Equal(t, output, "")
}

func TestProcessorEmptyJson(t *testing.T) {
	output, err := ProcessorEmptyJson(&cMap, "Pickle Rick!")
	require.Nil(t, err)
	require.Equal(t, output, "{}")

	output, err = ProcessorEmptyJson(&cMap, "{\"name\": \"Pickle Rick!\"}")
	require.Nil(t, err)
	require.Equal(t, output, "{}")
}

func TestRandomizeUUID(t *testing.T) {
	tempUUID := uuid.New().String()
	output, err := ProcessorRandomUUID(&cMap, tempUUID)
	require.Nil(t, err)
	require.NotEqual(t, output, tempUUID)

	output, err = ProcessorRandomUUID(&cMap, "")
	require.NotNil(t, err)
	require.Equal(t, output, "")
}
