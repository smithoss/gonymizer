package gonymizer

import (
	"bufio"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"unicode"
)

// maxLinesPerChunk bounds the number of lines per chunk.
const maxLinesPerChunk = 100000

// Chunk is a section of Postgres' data dump together with metadata.
// SubChunkNumber: 0 if chunk does not contain a COPY line or isn't a continuation of a COPY section from another chunk
// before it; >= 1 otherwise.
// DataBegins: the line number starting from which actual table data is defined.
type Chunk struct {
	Data           *strings.Builder
	SchemaName     string
	TableName      string
	ColumnNames    []string
	ChunkNumber    int
	SubChunkNumber int
	NumLines       int
	DataBegins     int
	Inclusive      bool
}

// Filename returns a filename for a chunk.
func (c Chunk) Filename() string {
	return fmt.Sprintf("%06d.%06d.part", c.ChunkNumber, c.SubChunkNumber)
}

// ProcessConfig contains the arguments required for processing a dump.
type ProcessConfig struct {
	DBMapper            *DBMapper
	DestinationFilename string
	GenerateSeed        bool
	Inclusive           bool
	NumWorkers          int
	PostprocessFilename string
	PreprocessFilename  string
	SourceFilename      string
}

// ColumnMapperContainer can return a reference to a ColumnMapper
type ColumnMapperContainer interface {
	ColumnMapper(schemaName, tableName, columnName string) *ColumnMapper
}

// StringWriter can write strings to a buffer/file/etc.
type StringWriter interface {
	WriteString(input string) (int, error)
}

// StringReader can write strings to a buffer/file/etc.
type StringReader interface {
	ReadString(delim byte) (string, error)
}

// ProcessConcurrently will process the supplied dump file concurrently according to the supplied database map file.
// generateSeed can also be set to true which will inform the function to use Go's built-in random number generator.
func ProcessConcurrently(config ProcessConfig) error {
	err := seedRNG(config.DBMapper, config.GenerateSeed)
	if err != nil {
		return err
	}

	srcFile, err := os.Open(config.SourceFilename)
	if err != nil {
		log.Error(err)
		return err
	}
	defer srcFile.Close()

	reader := bufio.NewReader(srcFile)

	var wg sync.WaitGroup
	chunks := make(chan Chunk, config.NumWorkers*2)

	// Start 1 worker to pull out chunks from the file and put it on a channel of size 2*N
	go createChunks(chunks, reader, &wg, config.Inclusive, maxLinesPerChunk)

	// Start N workers to read from the channel and process the chunks concurrently, writing results to file
	startChunkWorkers(config.DBMapper, &wg, config.NumWorkers, chunks)

	// Merge all partial results from file to final dst file, deleting the partial results
	wg.Wait()
	err = mergeFiles(config)
	if err != nil {
		log.Fatal(err)
	}

	return nil
}

// startChunkWorkers starts a number of workers to process chunks when they arrive in a given channel
func startChunkWorkers(mapper ColumnMapperContainer, wg *sync.WaitGroup, numWorkers int, chunks <-chan Chunk) {
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go startChunkWorker(chunks, wg, mapper)
		log.Infof("Worker %d started!", i+1)
	}
}

// createChunks takes a reader and splits it up into maxLinesPerChunk sized pieces. These pieces are sent
// through a channel.
func createChunks(chunks chan<- Chunk, reader *bufio.Reader, wg *sync.WaitGroup, inclusive bool, maxLinesPerChunk int) {
	defer close(chunks)

	defer wg.Done()
	wg.Add(1)

	var (
		schemaName    string
		tableName     string
		columnNames   []string
		chunkCount    int
		subchunkCount int
		eof           bool
		hasSubchunk   bool
	)

	for !eof {
		var (
			builder   strings.Builder
			lineIndex int
		)

		chunk := Chunk{
			Data:           &builder,
			ChunkNumber:    chunkCount,
			SubChunkNumber: subchunkCount,
			SchemaName:     schemaName,
			TableName:      tableName,
			ColumnNames:    columnNames,
			Inclusive:      inclusive,
		}

		for lineIndex = 0; lineIndex < maxLinesPerChunk; lineIndex++ {
			input, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					eof = true
				} else {
					log.Fatal(err)
				}
				break
			}

			builder.WriteString(input)

			trimmedInput := strings.TrimLeftFunc(input, unicode.IsSpace)
			if strings.HasPrefix(trimmedInput, StateChangeTokenBeginCopy) {
				pattern := `^COPY (?P<Schema>[a-zA-Z_]+)\."?(?P<TableName>\w+)"? \((?P<Columns>.*)\) .*`
				r := regexp.MustCompile(pattern)
				submatch := r.FindStringSubmatch(trimmedInput)
				if len(submatch) == 0 {
					log.Fatal("Regex doesn't match: ", trimmedInput)
				}

				schemaName = submatch[1]
				tableName = submatch[2]
				columnNames = strings.Split(submatch[3], ", ")

				subchunkCount++

				chunk.SchemaName = schemaName
				chunk.TableName = tableName
				chunk.ColumnNames = columnNames
				chunk.DataBegins = lineIndex + 1
				chunk.SubChunkNumber = subchunkCount

				hasSubchunk = true
			} else if strings.HasPrefix(trimmedInput, StateChangeTokenEndCopy) {
				subchunkCount = 0
				schemaName = ""
				tableName = ""
				columnNames = nil
				hasSubchunk = false
				break
			}

			if lineIndex == maxLinesPerChunk-1 && hasSubchunk {
				subchunkCount++
			}
		}

		chunk.NumLines = lineIndex
		chunks <- chunk

		if subchunkCount == 0 {
			chunkCount++
		}
	}

	log.Infof("Processed %d chunks", chunkCount-1)
}

// mergeFiles takes a destination filename and pre/post-process files and writes all part files to the destination file
// including any pre/post-processing file data.
func mergeFiles(config ProcessConfig) error {
	log.Info("Merging partial files...")

	dstFile, err := os.Create(config.DestinationFilename)
	if err != nil {
		log.Error(err)
		return err
	}
	defer dstFile.Close()

	if len(config.PreprocessFilename) > 0 {
		if err = fileInjector(config.PreprocessFilename, dstFile); err != nil {
			log.Error("Unable to run preProcessor")
			return err
		}
	}

	if _, err := dstFile.WriteString("SET session_replication_role = 'replica';\n"); err != nil {
		return err
	}

	pwd, err := os.Getwd()
	if err != nil {
		return err
	}

	matches, err := filepath.Glob("*.part")
	if err != nil {
		return err
	}

	for _, filename := range matches {
		path := filepath.Join(pwd, filename)
		partFile, err := os.Open(path)
		if err != nil {
			log.Error(err)
			return err
		}

		_, err = io.Copy(dstFile, partFile)
		if err != nil {
			log.Error(err)
			return err
		}

		partFile.Close()
		os.Remove(filename)
	}

	if len(config.PostprocessFilename) > 0 {
		if err = fileInjector(config.PostprocessFilename, dstFile); err != nil {
			return err
		}
	}

	if _, err := dstFile.WriteString("SET session_replication_role = 'origin';\n"); err != nil {
		return err
	}

	return nil
}

// startChunkWorkers takes a receive-only channel of chunks and processes each chunk, writing them to file.
func startChunkWorker(chunks <-chan Chunk, wg *sync.WaitGroup, mapper ColumnMapperContainer) {
	defer wg.Done()

	for chunk := range chunks {
		dst := chunk.Filename()

		dstFile, err := os.Create(dst)
		if err != nil {
			log.Fatal(err)
		}

		processChunk(chunk, dstFile, mapper)

		err = dstFile.Close()
		if err != nil {
			log.Fatal(err)
		}

		log.Infof("%s written to file", dst)
	}
}

// processChunk reads data from a Chunk and writes to a destination file handler.
func processChunk(chunk Chunk, dstFile StringWriter, mapper ColumnMapperContainer) {
	reader := bufio.NewReader(strings.NewReader(chunk.Data.String()))

	cmaps, err := getColumnMappers(mapper, chunk)
	if err != nil {
		log.Fatalf("%s: Please add to Map file", err.Error())
	}

	processFromReader(chunk, dstFile, reader, cmaps)
}

// processFromReader reads data from a StringReader line by line, processes it and sends it to a StringWriter
func processFromReader(chunk Chunk, writer StringWriter, reader StringReader, cmaps []*ColumnMapper) {
	for i := 0; i > -1; i++ {
		input, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}

		trimmedInput := strings.TrimLeftFunc(input, unicode.IsSpace)
		isEnd := strings.HasPrefix(trimmedInput, StateChangeTokenEndCopy)
		isEmpty := len(trimmedInput) == 0
		aboveData := i < chunk.DataBegins
		hasNoData := chunk.ColumnNames == nil

		if aboveData || isEnd || isEmpty || hasNoData {
			_, err = writer.WriteString(input)
			if err != nil {
				log.Fatal(err)
			}
			continue
		}

		output := processRowFromChunk(cmaps, input, chunk)
		_, err = writer.WriteString(output)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// getColumnMappers returns a slice of references to ColumnMappers corresponding to the columns of the given Chunk.
// Returns nil if given Chunk does not contain any schemas.
func getColumnMappers(mapper ColumnMapperContainer, chunk Chunk) ([]*ColumnMapper, error) {
	if len(chunk.ColumnNames) <= 0 {
		return nil, nil
	}

	result := make([]*ColumnMapper, 0, len(chunk.ColumnNames))

	for _, columnName := range chunk.ColumnNames {
		cmap := mapper.ColumnMapper(chunk.SchemaName, chunk.TableName, columnName)

		if cmap == nil && chunk.Inclusive {
			errorString := fmt.Sprintf("column '%s.%s.%s' does not exist", chunk.SchemaName, chunk.TableName, columnName)
			return nil, errors.New(errorString)
		}

		result = append(result, cmap)
	}

	return result, nil
}

// processRowFromChunk processes a data row from a given Chunk
func processRowFromChunk(cmaps []*ColumnMapper, inputLine string, chunk Chunk) string {
	rowValues := strings.Split(inputLine, "\t")
	outputValues := make([]string, 0, len(rowValues))

	for i, cmap := range cmaps {
		output := processRawValue(rowValues[i], chunk.ColumnNames[i], cmap)

		// Append the column to our new line
		outputValues = append(outputValues, output)
	}

	return strings.Join(outputValues, "\t")
}

// processRawValue takes a rawValue of a row.column from the dump and anonymizes it
func processRawValue(rawValue, columnName string, cmap *ColumnMapper) string {
	var (
		err        error
		escapeChar string
		output     string
	)

	// Check to see if the column has an escape char at the end of it.
	// If so cut it and keep it for later
	if strings.HasSuffix(rawValue, "\n") {
		escapeChar = "\n"
		rawValue = strings.Replace(rawValue, "\n", "", -1)
	}

	// If column value is nil or if this column is not mapped, keep the value and continue on
	if rawValue == "\\N" || cmap == nil {
		output = rawValue
	} else {
		output, err = processValue(cmap, rawValue)
		if err != nil {
			log.Debug("columnName: ", columnName)
			log.Fatal(err)
		}
	}
	// Add escape character back to column
	output += escapeChar

	return output
}
