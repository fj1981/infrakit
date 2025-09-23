package cyutil_test

import (
	"fmt"
	"testing"

	"github.com/duke-git/lancet/v2/strutil"
	"github.com/fj1981/infrakit/pkg/cyutil"
	"github.com/stretchr/testify/assert"
)

func TestReplaceSqlStringContent(t *testing.T) {
	t.Run("SingleLineTests", func(t *testing.T) {
		tests := []struct {
			name          string
			input         string
			expectedEnd   string
			wantProcessed string
			wantNextEnd   string
		}{
			{
				name:          "Q-quoted string with braces",
				input:         "aq'{hello}'",
				expectedEnd:   "",
				wantProcessed: "aq'{hello}",
				wantNextEnd:   "'",
			},
			{
				name:          "",
				input:         "'",
				expectedEnd:   "",
				wantProcessed: "",
				wantNextEnd:   "'",
			},
			{
				name:          "Q-quoted at end of line",
				input:         "SELECT q'",
				expectedEnd:   "",
				wantProcessed: "SELECT q'",
				wantNextEnd:   "",
			},

			{
				name:          "Simple single quoted string",
				input:         "SELECT 'abc' FROM table",
				expectedEnd:   "",
				wantProcessed: "SELECT ___STR___ FROM table",
				wantNextEnd:   "",
			},

			{
				name:          "Q-quoted string with braces",
				input:         "SELECT q'{hello}' FROM table",
				expectedEnd:   "",
				wantProcessed: "SELECT ___STR___ FROM table",
				wantNextEnd:   "",
			},
			{
				name:          "Unclosed Q-quoted string",
				input:         "SELECT q'[unclosed FROM table",
				expectedEnd:   "",
				wantProcessed: "SELECT ___STR___",
				wantNextEnd:   "]'",
			},

			{
				name:          "Multiple single quoted strings",
				input:         "SELECT 'abc' || 'def' FROM table",
				expectedEnd:   "",
				wantProcessed: "SELECT ___STR___ || ___STR___ FROM table",
				wantNextEnd:   "",
			},

			{
				name:          "Q-quoted string with brackets",
				input:         "SELECT q'[test]' FROM table",
				expectedEnd:   "",
				wantProcessed: "SELECT ___STR___ FROM table",
				wantNextEnd:   "",
			},
			{
				name:          "Q-quoted string with parentheses",
				input:         "SELECT q'(value)' FROM table",
				expectedEnd:   "",
				wantProcessed: "SELECT ___STR___ FROM table",
				wantNextEnd:   "",
			},
			{
				name:          "Q-quoted string with angle brackets",
				input:         "SELECT q'<data>' FROM table",
				expectedEnd:   "",
				wantProcessed: "SELECT ___STR___ FROM table",
				wantNextEnd:   "",
			},
			{
				name:          "Q-quoted string with custom delimiter",
				input:         "SELECT q'#custom#' FROM table",
				expectedEnd:   "",
				wantProcessed: "SELECT ___STR___ FROM table",
				wantNextEnd:   "",
			},
			{
				name:          "Unclosed single quoted string",
				input:         "SELECT 'unclosed FROM table",
				expectedEnd:   "",
				wantProcessed: "SELECT ___STR___",
				wantNextEnd:   "'",
			},

			{
				name:          "Escaped single quotes",
				input:         "SELECT 'It''s a test' FROM table",
				expectedEnd:   "",
				wantProcessed: "SELECT ___STR___ FROM table",
				wantNextEnd:   "",
			},
			{
				name:          "Invalid Q-quoted string",
				input:         "SELECT q'test' FROM table",
				expectedEnd:   "",
				wantProcessed: "SELECT ___STR___ FROM table",
				wantNextEnd:   "",
			},
			{
				name:          "Mixed string types",
				input:         "SELECT 'normal' || q'[special]' FROM table",
				expectedEnd:   "",
				wantProcessed: "SELECT ___STR___ || ___STR___ FROM table",
				wantNextEnd:   "",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				gotProcessed, gotNextEnd := cyutil.ReplaceSqlStringContent(tt.input, tt.expectedEnd)
				fmt.Printf("In:  %s (state: %s)\n", tt.input, tt.expectedEnd)
				fmt.Printf("Out: %s (next: %s)\n\n", gotProcessed, gotNextEnd)
				assert.Equal(t, tt.wantProcessed, gotProcessed, "processed line mismatch")
				assert.Equal(t, tt.wantNextEnd, gotNextEnd, "next expected end mismatch")
			})
		}
	})
}

func TestRemoveComments(t *testing.T) {
	t.Run("SingleLineComments", func(t *testing.T) {
		tests := []struct {
			name               string
			input              string
			inComment          bool
			wantOutput         string
			wantStillInComment bool
		}{
			{
				name:               "Simple single line comment",
				input:              "SELECT * FROM table -- this is a comment",
				inComment:          false,
				wantOutput:         "SELECT * FROM table ",
				wantStillInComment: false,
			},
			{
				name:               "Multiple single line comments",
				input:              "SELECT * -- comment 1 \n FROM table -- comment 2",
				inComment:          false,
				wantOutput:         "SELECT * ",
				wantStillInComment: false,
			},
			{
				name:               "Single line comment at start",
				input:              "-- This is a comment\nSELECT * FROM table",
				inComment:          false,
				wantOutput:         "",
				wantStillInComment: false,
			},
			{
				name:               "Dash characters in string",
				input:              "SELECT '-- not a comment' FROM table",
				inComment:          false,
				wantOutput:         "SELECT '-- not a comment' FROM table",
				wantStillInComment: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				gotOutput, gotStillInComment := cyutil.RemoveSqlComments(tt.input, tt.inComment)
				assert.Equal(t, tt.wantOutput, gotOutput, "output mismatch")
				assert.Equal(t, tt.wantStillInComment, gotStillInComment, "comment state mismatch")
			})
		}
	})

	t.Run("MultiLineComments", func(t *testing.T) {
		tests := []struct {
			name               string
			input              string
			inComment          bool
			wantOutput         string
			wantStillInComment bool
		}{
			{
				name:               "Complete multi-line comment",
				input:              "SELECT * FROM table /* this is a comment */",
				inComment:          false,
				wantOutput:         "SELECT * FROM table ",
				wantStillInComment: false,
			},
			{
				name:               "Unclosed multi-line comment",
				input:              "SELECT * FROM table /* this is a comment",
				inComment:          false,
				wantOutput:         "SELECT * FROM table ",
				wantStillInComment: true,
			},
			{
				name:               "Continuing multi-line comment with closure",
				input:              "*/ SELECT * FROM table",
				inComment:          true,
				wantOutput:         " SELECT * FROM table",
				wantStillInComment: false,
			},
			{
				name:               "Continuing multi-line comment without closure",
				input:              "still in comment",
				inComment:          true,
				wantOutput:         "",
				wantStillInComment: true,
			},
			{
				name:               "Multiple multi-line comments",
				input:              "SELECT * FROM /* comment 1 */ table /* comment 2 */",
				inComment:          false,
				wantOutput:         "SELECT * FROM  table ",
				wantStillInComment: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				gotOutput, gotStillInComment := cyutil.RemoveSqlComments(tt.input, tt.inComment)
				assert.Equal(t, tt.wantOutput, gotOutput, "output mismatch")
				assert.Equal(t, tt.wantStillInComment, gotStillInComment, "comment state mismatch")
			})
		}
	})

	t.Run("MixedComments", func(t *testing.T) {
		tests := []struct {
			name               string
			input              string
			inComment          bool
			wantOutput         string
			wantStillInComment bool
		}{
			{
				name:               "Multi-line comment followed by single-line comment",
				input:              "SELECT * FROM table /* multi-line */ -- single-line",
				inComment:          false,
				wantOutput:         "SELECT * FROM table  ",
				wantStillInComment: false,
			},
			{
				name:               "Single-line comment inside multi-line comment",
				input:              "SELECT * FROM table /* multi-line -- not a comment */",
				inComment:          false,
				wantOutput:         "SELECT * FROM table ",
				wantStillInComment: false,
			},
			{
				name:               "Multi-line comment inside string",
				input:              "SELECT '/* not a comment */' FROM table",
				inComment:          false,
				wantOutput:         "SELECT '/* not a comment */' FROM table",
				wantStillInComment: false,
			},
			{
				name:               "Nested multi-line comments",
				input:              "SELECT * FROM /* outer /* inner */ comment */ table",
				inComment:          false,
				wantOutput:         "SELECT * FROM  comment */ table",
				wantStillInComment: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				gotOutput, gotStillInComment := cyutil.RemoveSqlComments(tt.input, tt.inComment)
				assert.Equal(t, tt.wantOutput, gotOutput, "output mismatch")
				assert.Equal(t, tt.wantStillInComment, gotStillInComment, "comment state mismatch")
			})
		}
	})

	t.Run("EdgeCases", func(t *testing.T) {
		tests := []struct {
			name               string
			input              string
			inComment          bool
			wantOutput         string
			wantStillInComment bool
		}{
			{
				name:               "Empty string",
				input:              "",
				inComment:          false,
				wantOutput:         "",
				wantStillInComment: false,
			},
			{
				name:               "Empty string in comment",
				input:              "",
				inComment:          true,
				wantOutput:         "",
				wantStillInComment: true,
			},
			{
				name:               "Only comment markers",
				input:              "/**/",
				inComment:          false,
				wantOutput:         "",
				wantStillInComment: false,
			},
			{
				name:               "Only single-line comment marker",
				input:              "--",
				inComment:          false,
				wantOutput:         "",
				wantStillInComment: false,
			},
			{
				name:               "Incomplete multi-line comment marker",
				input:              "SELECT * FROM table /",
				inComment:          false,
				wantOutput:         "SELECT * FROM table /",
				wantStillInComment: false,
			},
			{
				name:               "Multiple comment starts",
				input:              "/* /* nested comment */",
				inComment:          false,
				wantOutput:         " ",
				wantStillInComment: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				gotOutput, gotStillInComment := cyutil.RemoveSqlComments(tt.input, tt.inComment)
				assert.Equal(t, tt.wantOutput, gotOutput, "output mismatch")
				assert.Equal(t, tt.wantStillInComment, gotStillInComment, "comment state mismatch")
			})
		}
	})
}

func TestRemoveSqlComments(t *testing.T) {
	aa := strutil.PadStart("7.58µs", 10, " ")
	fmt.Printf("[%s]%d\n", aa, len(aa))
	bb := strutil.PadStart("7.58us", 10, " ")
	fmt.Printf("[%s]%d\n", bb, len(bb))
}
