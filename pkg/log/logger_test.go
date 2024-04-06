package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_getCaller(t *testing.T) {
	tests := []struct {
		name string
		skip int
		file string
		line int32
	}{
		{
			name: "getCaller(1)",
			skip: 1,
			file: "log/logger_test.go",
			line: 39,
		},
		{
			name: "getCaller(2)",
			skip: 2,
			file: "log/logger_test.go",
			line: 31,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, line := callTestMethod(tt.skip)
			assert.Equal(t, tt.file, file)
			assert.Equal(t, tt.line, line)
		})
	}
}

func callTestMethod(skip int) (string, int32) {
	return getCaller(skip)
}
