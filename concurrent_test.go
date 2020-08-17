package gonymizer

import (
	"bufio"
	"github.com/stretchr/testify/require"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// MockDBMapper that has implementation of ColumnMapper function
type MockDBMapper struct{}

// mockColumnMapper to check address of in tests
var mockColumnMapper = ColumnMapper{
	Processors: []ProcessorDefinition{
		{Name: "ScrubString"},
	},
}

// mock ColumnMapper implementation that returns nil on everything but "foo" for columnName
func (m MockDBMapper) ColumnMapper(schemaName, tableName, columnName string) *ColumnMapper {
	if columnName == "foo" {
		return &mockColumnMapper
	}

	return nil
}

// MockReaderWriter mocks the Reader and Writer of files in one struct and has a buffer to test expected output.
type MockReaderWriter struct {
	ReadBuffer          []string
	WriteBuffer         []string
	ExpectedWriteBuffer []string
	Index               int
}

func (rw *MockReaderWriter) ReadString(delim byte) (string, error) {
	if rw.Index < len(rw.ReadBuffer) {
		result := rw.ReadBuffer[rw.Index]
		rw.Index++
		return result, nil
	}
	return "", io.EOF
}

func (rw *MockReaderWriter) WriteString(input string) (int, error) {
	rw.WriteBuffer = append(rw.WriteBuffer, input)
	return 0, nil
}

func (rw *MockReaderWriter) CheckExpected(t *testing.T) {
	require.Equal(t, rw.ExpectedWriteBuffer, rw.WriteBuffer)
}

func Test_createChunks(t *testing.T) {
	type args struct {
		reader           *bufio.Reader
		inclusive        bool
		maxLinesPerChunk int
	}
	type testCase struct {
		name           string
		expectedChunks int32
		args           args
	}

	testData := `--
-- 
COPY public.foo_foo (id, name) FROM stdin;
1	A
2	B
\.

COPY public.bar_bar (id, name) FROM stdin;
1	C
2	D
\.

`
	tests := []testCase{
		{
			name: "can chunk with no split sections",
			args: args{
				reader:           bufio.NewReader(strings.NewReader(testData)),
				inclusive:        true,
				maxLinesPerChunk: 6,
			},
			// 1. First comment till end token
			// 2. First empty line till second end token
			// 3. Second empty line
			expectedChunks: 3,
		},
		{
			name: "can chunk with split sections",
			args: args{
				reader:           bufio.NewReader(strings.NewReader(testData)),
				inclusive:        true,
				maxLinesPerChunk: 2,
			},
			// 1. Two comment lines
			// 2. First COPY + 1 A line
			// 3. 2 B lines + first end token
			// 4. First empty line + second COPY
			// 5. 1 C + 2 D lines
			// 6. Second end token
			// 7. Second empty line
			expectedChunks: 7,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				chunkCount int32
				wg         sync.WaitGroup
			)
			var chunks = make(chan Chunk)
			arg := tt.args

			go createChunks(chunks, arg.reader, &wg, arg.inclusive, arg.maxLinesPerChunk)

			wg.Add(1)
			go func() {
				defer wg.Done()
				for chunk := range chunks {
					atomic.AddInt32(&chunkCount, 1)

					if chunk.SubChunkNumber >= 1 {
						require.NotEmpty(t, chunk.ColumnNames)
						require.NotEmpty(t, chunk.TableName)
						require.NotEmpty(t, chunk.SchemaName)
					} else {
						require.Empty(t, chunk.ColumnNames)
						require.Empty(t, chunk.TableName)
						require.Empty(t, chunk.SchemaName)
					}
				}
			}()

			wg.Wait()

			require.Equal(t, tt.expectedChunks, chunkCount)
		})
	}
}

func Test_getColumnMappers(t *testing.T) {
	type args struct {
		mapper ColumnMapperContainer
		chunk  Chunk
	}
	tests := []struct {
		name    string
		args    args
		want    []*ColumnMapper
		wantErr bool
	}{
		{
			name: "returns nil if Chunk has empty .ColumnNames",
			args: args{
				mapper: MockDBMapper{},
				chunk: Chunk{
					ColumnNames: make([]string, 0),
				},
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "returns error if Chunk is Inclusive but ColumnMapper is nil",
			args: args{
				mapper: MockDBMapper{},
				chunk: Chunk{
					ColumnNames: []string{"bar"},
					Inclusive:   true,
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "returns equal number of columnMappers",
			args: args{
				mapper: MockDBMapper{},
				chunk: Chunk{
					ColumnNames: []string{"foo", "foo", "foo"},
					Inclusive:   true,
				},
			},
			want:    []*ColumnMapper{&mockColumnMapper, &mockColumnMapper, &mockColumnMapper},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getColumnMappers(tt.args.mapper, tt.args.chunk)
			if (err != nil) != tt.wantErr {
				t.Errorf("getColumnMappers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getColumnMappers() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_processRawValue(t *testing.T) {
	type args struct {
		rawValue   string
		columnName string
		cmap       *ColumnMapper
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "keeps newliness intact",
			args: args{
				rawValue:   "a\n",
				columnName: "foo",
				cmap: &ColumnMapper{
					Processors: []ProcessorDefinition{
						{Name: "Identity"},
					},
				},
			},
			want: "a\n",
		},
		{
			name: "processes if cmap is not nil",
			args: args{
				rawValue:   "a",
				columnName: "foo",
				cmap:       &mockColumnMapper,
			},
			want: "*",
		},
		{
			name: "keeps value if cmap is nil",
			args: args{
				rawValue:   "a",
				columnName: "foo",
				cmap:       nil,
			},
			want: "a",
		},
		{
			name: "keeps value if raw value is \\N",
			args: args{
				rawValue:   "\\N",
				columnName: "foo",
				cmap:       &mockColumnMapper,
			},
			want: "\\N",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := processRawValue(tt.args.rawValue, tt.args.columnName, tt.args.cmap); got != tt.want {
				t.Errorf("processRawValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_processRowFromChunk(t *testing.T) {
	type args struct {
		cmaps     []*ColumnMapper
		inputLine string
		chunk     Chunk
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "anonymizes row correctly",
			args: args{
				cmaps: []*ColumnMapper{
					&mockColumnMapper,
					&mockColumnMapper,
				},
				inputLine: "aaa\tbbb\n",
				chunk: Chunk{
					ColumnNames: []string{"foo", "foo"},
				},
			},
			want: "***\t***\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := processRowFromChunk(tt.args.cmaps, tt.args.inputLine, tt.args.chunk); got != tt.want {
				t.Errorf("processRowFromChunk() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_processFromReader(t *testing.T) {
	type args struct {
		chunk        Chunk
		readerWriter MockReaderWriter
		cmaps        []*ColumnMapper
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "does not modify lines above data",
			args: args{
				chunk: Chunk{
					DataBegins:  100,
					ColumnNames: []string{"foo", "foo"},
				},
				readerWriter: MockReaderWriter{
					ReadBuffer: []string{
						"abc",
						"\tdef",
						"ghi\n",
					},
					WriteBuffer: make([]string, 0),
					ExpectedWriteBuffer: []string{
						"abc",
						"\tdef",
						"ghi\n",
					},
				},
				cmaps: nil,
			},
		},
		{
			name: "copies end line",
			args: args{
				chunk: Chunk{
					DataBegins:  0,
					ColumnNames: []string{"foo", "foo"},
				},
				readerWriter: MockReaderWriter{
					ReadBuffer: []string{
						"\\.",
					},
					WriteBuffer: make([]string, 0),
					ExpectedWriteBuffer: []string{
						"\\.",
					},
				},
				cmaps: nil,
			},
		},
		{
			name: "copies lines with just whitespace",
			args: args{
				chunk: Chunk{
					DataBegins:  0,
					ColumnNames: []string{"foo", "foo"},
				},
				readerWriter: MockReaderWriter{
					ReadBuffer: []string{
						"     ",
					},
					WriteBuffer: make([]string, 0),
					ExpectedWriteBuffer: []string{
						"     ",
					},
				},
				cmaps: nil,
			},
		},
		{
			name: "copies lines that have no data",
			args: args{
				chunk: Chunk{
					DataBegins:  0,
					ColumnNames: nil,
				},
				readerWriter: MockReaderWriter{
					ReadBuffer: []string{
						"abc",
						"\tdef",
						"ghi\n",
					},
					WriteBuffer: make([]string, 0),
					ExpectedWriteBuffer: []string{
						"abc",
						"\tdef",
						"ghi\n",
					},
				},
				cmaps: nil,
			},
		},
		{
			name: "processes all data from chunk",
			args: args{
				chunk: Chunk{
					DataBegins:  0,
					ColumnNames: []string{"foo", "foo"},
				},
				readerWriter: MockReaderWriter{
					ReadBuffer: []string{
						"aaa\tbbb\n",
						"ccc\tddd\n",
					},
					WriteBuffer: make([]string, 0),
					ExpectedWriteBuffer: []string{
						"***\t***\n",
						"***\t***\n",
					},
				},
				cmaps: []*ColumnMapper{
					&mockColumnMapper,
					&mockColumnMapper,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processFromReader(tt.args.chunk, &tt.args.readerWriter, &tt.args.readerWriter, tt.args.cmaps)
			tt.args.readerWriter.CheckExpected(t)
		})
	}
}
