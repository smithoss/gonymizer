package gonymizer

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/icrowley/fake"
)

// All processors are designed to work "unseeded"
// Make sure something seeds the RNG before you call the top level process function.

// in order for the processor to "find" the functions it's got to
// 1. conform to ProcessorFunc
// 2. be in the processor map

// There are fancy ways for the reflection/runtime system to find functions
// that match certain text patters, like how the system finds TestX(*t.Testing) funcs
// but we dont' need that.  just put them in the map to make my life easy please.

// The number of times to check the input string for similarity to the output string. We want to keep this at a distance
// of 0.4 or higher. Please see: https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance
//const jaroWinklerAttempts = 1000

// lookup string for random lowercase letters
const lowercaseSet = "abcdefghijklmnopqrstuvwxyz"

// lookup string for random uppercase letters
const uppercaseSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

// lookup string for random integers
const numericSet = "0123456789"

const lowercaseSetLen = 26
const uppercaseSetLen = 26
const numericSetLen = 10

// safeAlphaNumericMap is a concurrency-safe map[string]map[string][string]
type safeAlphaNumericMap struct {
	v   map[string]map[string]string
	mux sync.Mutex
}

// Get returns a string that an input is mapped to under a parentKey.
func (c *safeAlphaNumericMap) Get(parentKey, input string) (string, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	anMap, ok := c.v[parentKey]
	if !ok {
		anMap = map[string]string{}
		c.v[parentKey] = anMap
	}

	result, ok := anMap[input]
	if !ok {
		result = scrambleString(input)
		anMap[input] = result
	}

	return result, nil
}

// safeUUIDMap is a concurrency-safe map[uuid.UUID]uuid.UUID
type safeUUIDMap struct {
	v   map[uuid.UUID]uuid.UUID
	mux sync.Mutex
}

// Get returns a mapped uuid for a given UUID if it has already previously been anonymized and a new UUID otherwise.
func (c *safeUUIDMap) Get(key uuid.UUID) (string, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	result, ok := c.v[key]
	if !ok {
		result, err := uuid.NewRandom()
		if err != nil {
			return "", err
		}

		c.v[key] = result
	}

	return result.String(), nil
}

// ProcessorCatalog is the function map that points to each Processor to it's entry function. All Processors are listed
// in this map.
var ProcessorCatalog map[string]ProcessorFunc

// AlphaNumericMap is used to keep consistency with scrambled alpha numeric strings.
// For example, if we need to scramble things such as Social Security Numbers, but it is nice to keep track of these
// changes so if we run across the same SSN again we can scramble it to what we already have.
var AlphaNumericMap = safeAlphaNumericMap{
	v: make(map[string]map[string]string),
}

// UUIDMap is the Global UUID map for all UUIDs that we anonymize. Similar to AlphaNumericMap this map contains all
// UUIDs and what they are changed to. Some tables use UUIDs as the primary key and this allows us to keep consistency
// in the data set when anonymizing it.
var UUIDMap = safeUUIDMap{
	v: make(map[uuid.UUID]uuid.UUID),
}

// init initializes the ProcessorCatalog map for all processors. A processor must be listed here to be accessible.
func init() {
	ProcessorCatalog = map[string]ProcessorFunc{
		"AlphaNumericScrambler": ProcessorAlphaNumericScrambler,
		"EmptyJson":             ProcessorEmptyJson,
		"FakeStreetAddress":     ProcessorAddress,
		"FakeCity":              ProcessorCity,
		"FakeCompanyName":       ProcessorCompanyName,
		"FakeEmailAddress":      ProcessorEmailAddress,
		"FakeFirstName":         ProcessorFirstName,
		"FakeFullName":          ProcessorFullName,
		"FakeIPv4":              ProcessorIPv4,
		"FakeLastName":          ProcessorLastName,
		"FakePhoneNumber":       ProcessorPhoneNumber,
		"FakeState":             ProcessorState,
		"FakeStateAbbrev":       ProcessorStateAbbrev,
		"FakeUsername":          ProcessorUserName,
		"FakeZip":               ProcessorZip,
		"Identity":              ProcessorIdentity, // Default: Does not modify field
		"RandomBoolean":         ProcessorRandomBoolean,
		"RandomDate":            ProcessorRandomDate,
		"RandomDigits":          ProcessorRandomDigits,
		"RandomUUID":            ProcessorRandomUUID,
		"ScrubString":           ProcessorScrubString,
	}

}

// ProcessorFunc is a simple function prototype for the ProcessorMap function pointers.
type ProcessorFunc func(*ColumnMapper, string) (string, error)

// fakeFuncPtr is a simple function prototype for function pointers to the Fake package's fake functions.
//type fakeFuncPtr func() string

// ProcessorAlphaNumericScrambler will receive the column metadata via ColumnMap and the column's actual data via the
// input string. The processor will scramble all alphanumeric digits and characters, but it will leave all
// non-alphanumerics the same without modification. These values are globally mapped and use the AlphaNumericMap to
// remap values once they are seen more than once.
//
// Example:
// "PUI-7x9vY" = ProcessorAlphaNumericScrambler("ABC-1a2bC")
func ProcessorAlphaNumericScrambler(cmap *ColumnMapper, input string) (string, error) {
	if cmap.ParentSchema != "" && cmap.ParentTable != "" && cmap.ParentColumn != "" {
		parentKey := fmt.Sprintf("%s.%s.%s", cmap.ParentSchema, cmap.ParentTable, cmap.ParentColumn)
		return AlphaNumericMap.Get(parentKey, input)
	} else {
		return scrambleString(input), nil
	}
}

// ProcessorAddress will return a fake address string that is compiled from the fake library
func ProcessorAddress(cmap *ColumnMapper, input string) (string, error) {
	return fake.StreetAddress(), nil
}

// ProcessorCity will return a real city name that is >= 0.4 Jaro-Winkler similar than the input.
func ProcessorCity(cmap *ColumnMapper, input string) (string, error) {
	return fake.City(), nil
}

// ProcessorEmailAddress will return an e-mail address that is >= 0.4 Jaro-Winkler similar than the input.
func ProcessorEmailAddress(cmap *ColumnMapper, input string) (string, error) {
	return fake.EmailAddress(), nil
}

// ProcessorFirstName will return a first name that is >= 0.4 Jaro-Winkler similar than the input.
func ProcessorFirstName(cmap *ColumnMapper, input string) (string, error) {
	return fake.FirstName(), nil
}

// ProcessorFullName will return a full name that is >= 0.4 Jaro-Winkler similar than the input.
func ProcessorFullName(cmap *ColumnMapper, input string) (string, error) {
	return fake.FullName(), nil
}

// ProcessorIdentity will skip anonymization and leave output === input.
func ProcessorIdentity(cmap *ColumnMapper, input string) (string, error) {
	return input, nil
}

func ProcessorIPv4(cmap *ColumnMapper, input string) (string, error) {
	return fake.IPv4(), nil
}

// ProcessorLastName will return a last name that is >= 0.4 Jaro-Winkler similar than the input.
func ProcessorLastName(cmap *ColumnMapper, input string) (string, error) {
	return fake.LastName(), nil
}

// ProcessorEmptyJson will return an empty JSON no matter what is the input.
func ProcessorEmptyJson(cmap *ColumnMapper, input string) (string, error) {
	return "{}", nil
}

// ProcessorPhoneNumber will return a phone number that is >= 0.4 Jaro-Winkler similar than the input.
func ProcessorPhoneNumber(cmap *ColumnMapper, input string) (string, error) {
	return fake.Phone(), nil
}

// ProcessorState will return a state that is >= 0.4 Jaro-Winkler similar than the input.
func ProcessorState(cmap *ColumnMapper, input string) (string, error) {
	return fake.State(), nil
}

// ProcessorStateAbbrev will return a state abbreviation.
func ProcessorStateAbbrev(cmap *ColumnMapper, input string) (string, error) {
	return fake.StateAbbrev(), nil
}

// ProcessorUserName will return a username that is >= 0.4 Jaro-Winkler similar than the input.
func ProcessorUserName(cmap *ColumnMapper, input string) (string, error) {
	return fake.UserName(), nil
}

// ProcessorZip will return a zip code that is >= 0.4 Jaro-Winkler similar than the input.
func ProcessorZip(cmap *ColumnMapper, input string) (string, error) {
	return fake.Zip(), nil
}

// ProcessorCompanyName will return a company name that is >= 0.4 Jaro-Winkler similar than the input.
func ProcessorCompanyName(cmap *ColumnMapper, input string) (string, error) {
	return fake.Company(), nil
}

// ProcessorRandomBoolean will return a random boolean value.
func ProcessorRandomBoolean(cmap *ColumnMapper, input string) (string, error) {
	var randomBoolean string = "FALSE"
	if rand.Intn(2) == 0 {
		randomBoolean = "TRUE"
	}
	return randomBoolean, nil
}

// ProcessorRandomDate will return a random day and month, but keep year the same (See: HIPAA rules)
func ProcessorRandomDate(cmap *ColumnMapper, input string) (string, error) {
	// ISO 8601/SQL standard ->  2018-08-28
	dateSplit := strings.Split(input, "-")

	if len(dateSplit) < 3 || len(dateSplit) > 3 {
		return "", fmt.Errorf("Date format is not ISO-8601: %q", dateSplit)
	}

	// Parse Year
	year, err := strconv.Atoi(dateSplit[0])
	if err != nil {
		return "", fmt.Errorf("Unable to parse year from date: %q", dateSplit)
	}

	// NOTE: HIPAA only requires we scramble month and day, not year
	scrambledDate := randomizeDate(year)
	return scrambledDate, nil
}

// ProcessorRandomDigits will return a random string of digit(s) keeping the same length of the input.
func ProcessorRandomDigits(cmap *ColumnMapper, input string) (string, error) {
	return fake.DigitsN(len(input)), nil
}

// ProcessorRandomUUID will generate a random UUID and replace the input with the new UUID. The input however will be
// mapped to the output so every occurrence of the input UUID will replace it with the same output UUID that was
// originally created during the first occurrence of the input UUID.
func ProcessorRandomUUID(cmap *ColumnMapper, input string) (string, error) {
	var scrambledUUID string

	inputID, err := uuid.Parse(input)

	if err != nil {
		scrambledUUID = ""
	} else {
		scrambledUUID, err = randomizeUUID(inputID)
	}

	return scrambledUUID, err
}

// ProcessorScrubString will replace the input string with asterisks (*). Useful for blanking out password fields.
func ProcessorScrubString(cmap *ColumnMapper, input string) (string, error) {
	return scrubString(input), nil
}

/*
func jaroWinkler(input string, jwDistance float64, faker fakeFuncPtr) (output string, err error) {
	for counter := 0; counter < jaroWinklerAttempts; counter++ {
		output = faker()
		if jw := matchr.JaroWinkler(input, output, true); jw > jwDistance {
			return output, nil
		}
	}
	return output, fmt.Errorf("Jaro-Winkler: distance < %e for %d attempts. Input: %s, Output: %s",
		jwDistance, jaroWinklerAttempts, input, output)
}
*/

// randomizeUUID creates a random UUID and adds it to the map of input->output. If input already exists it returns
// the output that was previously calculated for input.
func randomizeUUID(input uuid.UUID) (string, error) {
	return UUIDMap.Get(input)
}

// randomizeDate randomizes a day and month for a given year. This function is leap year compatible.
func randomizeDate(year int) string {
	// To find the length of the randomly selected month we need to find the last day of the month.
	// See: https://yourbasic.org/golang/last-day-month-date/

	randMonth := rand.Intn(12) + 1
	monthMaxDay := date(year, randMonth, 0).Day()
	randDay := rand.Intn(monthMaxDay) + 1
	fullDateTime := date(year, randMonth, randDay).Format("2006-01-02")

	return fullDateTime
}

// date returns the date for a given year, month, day. Used to check validity of supplied date.
func date(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

// scrambleString will replace capital letters with a random capital letter, a lower-case letter with a random
// lower-case letter, and numbers with a random number. String size will be the same length and non-alphanumerics will
// be ignored in the input and output.
func scrambleString(input string) string {
	var b strings.Builder

	for i := 0; i < len(input); i++ {
		switch c := input[i]; {
		case c == '\\':
			b.WriteByte(c)
			i = passEscapeSequence(b.WriteByte, input, i+1)
		case c >= 'a' && c <= 'z':
			b.WriteString(randomLowercase())
		case c >= 'A' && c <= 'Z':
			b.WriteString(randomUppercase())
		case c >= '0' && c <= '9':
			b.WriteString(randomNumeric())
		default:
			b.WriteByte(c)
		}
	}

	return b.String()
}

// Retain escapes sequences such as \n, \xHH as they are.
// See https://www.postgresql.org/docs/current/sql-syntax-lexical.html
// We liberally accept
// - any escaped character as is
// - numeric sequences of lengths shorter than expected
// as more of a responsibility of the producer and consumer.
// We return index of the last consumed input character.
func passEscapeSequence(write func(c byte) error, input string, i int) int {
	c := input[i]
	write(c)
	switch {
	case c >= '0' && c <= '7':
		i = passOctalSequence(write, input, i+1)
	case c == 'x':
		i = passHexadecimalSequence(write, input, i+1, 2)
	case c == 'u':
		i = passHexadecimalSequence(write, input, i+1, 4)
	case c == 'U':
		i = passHexadecimalSequence(write, input, i+1, 8)
	}
	return i
}

func passOctalSequence(write func(c byte) error, input string, i int) int {
	for endAt := i + 2; i < endAt && i < len(input) && input[i] >= '0' && input[i] <= '7'; i++ {
		write(input[i])
	}
	return i - 1
}

func passHexadecimalSequence(write func(c byte) error, input string, i int, maxlen int) int {
	for endAt := i + maxlen; i < endAt && i < len(input); i++ {
		c := input[i]
		if !isHexadecimalCharacter(c) {
			break
		}
		write(c)
	}
	return i - 1
}

func isHexadecimalCharacter(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F')
}

// scrubString replaces the input string with asterisks (*) and returns it as the output.
func scrubString(input string) string {
	return strings.Repeat("*", utf8.RuneCountInString(input))
}

// randomLowercase will pick a random location in the lowercase constant string and return the letter at that position.
func randomLowercase() string {
	return string(lowercaseSet[rand.Intn(lowercaseSetLen)])
}

// randomUppercase will pick a random location in the uppercase constant string and return the letter at that position.
func randomUppercase() string {
	return string(uppercaseSet[rand.Intn(uppercaseSetLen)])
}

// randomNumeric will return a random location in the numeric constant string and return the number at that position.
func randomNumeric() string {
	return string(numericSet[rand.Intn(numericSetLen)])
}
