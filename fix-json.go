package jsonrepair

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
)

// Precompiled regexes for performance
var (
	markdownOpeningFence = regexp.MustCompile(`^` + "```" + `(?:json|JSON)?\s*\n?`)
	markdownClosingFence = regexp.MustCompile(`\n?` + "```" + `\s*$`)
	trailingCommaPattern = regexp.MustCompile(`,(\s*[\]}])`)
)

// FixJSON implements the JSON repair cascade with three layers:
// Layer 1: Pre-processing (markdown removal, quote normalization)
// Layer 2: Structural repair (bracket balancing, trailing commas)
// Layer 3: Extraction and validation
func FixJSON(ctx context.Context, req *FixJSONRequest) (*FixJSONResponse, error) {
	if req == nil || req.JsonString == "" {
		return &FixJSONResponse{
			RepairedJSON: "",
			Valid:        false,
			Errors:       []string{"empty input"},
		}, nil
	}

	var errors []string
	original := req.JsonString

	// Layer 0: Extract potential JSON from surrounding text
	extracted := extractJSON(original)
	if extracted != original {
		errors = append(errors, "extracted JSON from surrounding text")
	}

	// Layer 1: Pre-processing
	repaired := stripMarkdownFencing(extracted)
	repaired = normalizeSmartQuotes(repaired)

	// Try parsing before expensive repairs
	if isValidJSON(repaired) {
		minified := minifyJSON(repaired)
		return &FixJSONResponse{
			RepairedJSON: minified,
			Valid:        true,
			Errors:       errors, // Preserve any errors from extraction
		}, nil
	}

	// Layer 2: Apply structural repairs
	repaired = replaceConstantsStringAware(repaired)
	repaired = convertObjectKeySingleQuotes(repaired)
	repaired = removeTrailingCommasStringAware(repaired)
	repaired = quoteUnquotedKeys(repaired)
	repaired = cleanErroneousBracketsInStrings(repaired)
	repaired, bracketWarnings := balanceBrackets(repaired)
	errors = append(errors, bracketWarnings...)

	// Layer 3: Validate and return
	if isValidJSON(repaired) {
		minified := minifyJSON(repaired)
		return &FixJSONResponse{
			RepairedJSON: minified,
			Valid:        true,
			Errors:       errors,
		}, nil
	}

	// Failed to repair - return best effort with errors
	errors = append(errors, "failed to produce valid JSON after all repairs")
	return &FixJSONResponse{
		RepairedJSON: repaired,
		Valid:        false,
		Errors:       errors,
	}, nil
}

// ===========================
// Layer 0: Extraction
// ===========================

// extractJSON finds the first complete JSON object or array in the input
// and strips any surrounding text
func extractJSON(s string) string {
	s = strings.TrimSpace(s)

	// Find first { or [
	startBrace := strings.Index(s, "{")
	startBracket := strings.Index(s, "[")

	start := -1
	openingChar := ' '
	if startBrace >= 0 && startBracket >= 0 {
		if startBrace < startBracket {
			start = startBrace
			openingChar = '{'
		} else {
			start = startBracket
			openingChar = '['
		}
	} else if startBrace >= 0 {
		start = startBrace
		openingChar = '{'
	} else if startBracket >= 0 {
		start = startBracket
		openingChar = '['
	}

	if start < 0 {
		return s // No JSON found, return original
	}

	// Find the end of the JSON structure by matching brackets
	end := findJSONEnd(s, start, openingChar)
	if end < 0 {
		// Couldn't find end, return from start to end of string
		return strings.TrimSpace(s[start:])
	}

	// Extract just the JSON portion
	return strings.TrimSpace(s[start : end+1])
}

// findJSONEnd finds the closing bracket/brace that matches the opening character
// Uses a stack to ensure bracket types match correctly
// Handles both double-quoted and single-quoted strings (since we're repairing malformed JSON)
// Returns the LAST position where the stack becomes empty, to handle garbage brackets
func findJSONEnd(s string, start int, openingChar rune) int {
	var stack []rune
	inDoubleQuote := false
	inSingleQuote := false
	escaped := false
	lastEmptyStackPos := -1

	for i := start; i < len(s); i++ {
		char := rune(s[i])

		if escaped {
			escaped = false
			continue
		}

		if char == '\\' {
			escaped = true
			continue
		}

		if char == '"' && !inSingleQuote {
			inDoubleQuote = !inDoubleQuote
			continue
		}

		if char == '\'' && !inDoubleQuote {
			inSingleQuote = !inSingleQuote
			continue
		}

		if inDoubleQuote || inSingleQuote {
			continue
		}

		// Track bracket/brace depth with type checking
		switch char {
		case '{', '[':
			stack = append(stack, char)
		case '}':
			if len(stack) > 0 && stack[len(stack)-1] == '{' {
				stack = stack[:len(stack)-1]
				if len(stack) == 0 {
					lastEmptyStackPos = i // Remember this position, but keep scanning
				}
			}
			// For mismatched brackets, continue scanning (they might be garbage in malformed JSON)
		case ']':
			if len(stack) > 0 && stack[len(stack)-1] == '[' {
				stack = stack[:len(stack)-1]
				if len(stack) == 0 {
					lastEmptyStackPos = i // Remember this position, but keep scanning
				}
			}
			// For mismatched brackets, continue scanning (they might be garbage in malformed JSON)
		}
	}

	return lastEmptyStackPos // Return the last position where stack was empty
}

// ===========================
// Layer 1: Pre-processing
// ===========================

// stripMarkdownFencing removes markdown code fences like ```json ... ```
func stripMarkdownFencing(s string) string {
	s = strings.TrimSpace(s)
	s = markdownOpeningFence.ReplaceAllString(s, "")
	s = markdownClosingFence.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

// normalizeSmartQuotes replaces smart quotes with standard quotes
// Only handles Unicode smart quotes, does NOT convert single to double
func normalizeSmartQuotes(s string) string {
	s = strings.ReplaceAll(s, "\u201c", "\"") // Left double quotation mark
	s = strings.ReplaceAll(s, "\u201d", "\"") // Right double quotation mark
	s = strings.ReplaceAll(s, "\u2018", "'")  // Left single quotation mark → regular single
	s = strings.ReplaceAll(s, "\u2019", "'")  // Right single quotation mark → regular single
	return s
}

// ===========================
// Layer 2: Structural Repair
// ===========================

// replaceConstantsStringAware replaces Python/JS constants ONLY outside strings
func replaceConstantsStringAware(s string) string {
	var result strings.Builder
	result.Grow(len(s))

	inString := false
	escaped := false

	for i := 0; i < len(s); i++ {
		char := s[i]

		if escaped {
			result.WriteByte(char)
			escaped = false
			continue
		}

		if char == '\\' {
			result.WriteByte(char)
			escaped = true
			continue
		}

		if char == '"' {
			inString = !inString
			result.WriteByte(char)
			continue
		}

		if inString {
			result.WriteByte(char)
			continue
		}

		// Outside strings: check for constants
		// Try to match None, True, False, undefined
		matched := false

		if i+4 <= len(s) && s[i:i+4] == "None" && (i+4 >= len(s) || !isIdentifierChar(s[i+4])) {
			if i == 0 || !isIdentifierChar(s[i-1]) {
				result.WriteString("null")
				i += 3 // Will be incremented by loop
				matched = true
			}
		} else if i+4 <= len(s) && s[i:i+4] == "True" && (i+4 >= len(s) || !isIdentifierChar(s[i+4])) {
			if i == 0 || !isIdentifierChar(s[i-1]) {
				result.WriteString("true")
				i += 3
				matched = true
			}
		} else if i+5 <= len(s) && s[i:i+5] == "False" && (i+5 >= len(s) || !isIdentifierChar(s[i+5])) {
			if i == 0 || !isIdentifierChar(s[i-1]) {
				result.WriteString("false")
				i += 4
				matched = true
			}
		} else if i+9 <= len(s) && s[i:i+9] == "undefined" && (i+9 >= len(s) || !isIdentifierChar(s[i+9])) {
			if i == 0 || !isIdentifierChar(s[i-1]) {
				result.WriteString("null")
				i += 8
				matched = true
			}
		}

		if !matched {
			result.WriteByte(char)
		}
	}

	return result.String()
}

func isIdentifierChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

// convertObjectKeySingleQuotes converts outer single quotes to double quotes for both keys and values
// Pattern: {'key': 'value'} → {"key": "value"}
// Leaves internal apostrophes alone: {'name': 'Dinson's App'} → {"name": "Dinson's App"}
func convertObjectKeySingleQuotes(s string) string {
	var result strings.Builder
	result.Grow(len(s))

	inDoubleQuote := false
	escaped := false

	for i := 0; i < len(s); i++ {
		char := s[i]

		if escaped {
			result.WriteByte(char)
			escaped = false
			continue
		}

		if char == '\\' {
			result.WriteByte(char)
			escaped = true
			continue
		}

		if char == '"' {
			inDoubleQuote = !inDoubleQuote
			result.WriteByte(char)
			continue
		}

		if inDoubleQuote {
			result.WriteByte(char)
			continue
		}

		// Outside double-quoted strings: detect single-quoted strings as pairs
		if char == '\'' {
			// Check if this is the start of a JSON string (key or value)
			if looksLikeJSONStringStart(s, i) {
				// Find the closing quote by context (handles internal apostrophes)
				closingQuotePos := findClosingSingleQuoteByContext(s, i)
				if closingQuotePos > i {
					// Convert opening quote to double
					result.WriteByte('"')
					// Copy everything between the quotes as-is (including internal apostrophes)
					i++
					for i < closingQuotePos {
						result.WriteByte(s[i])
						i++
					}
					// Convert closing quote to double
					result.WriteByte('"')
					// i is now at closingQuotePos, loop will increment it
					continue
				}
			}
			// Not a JSON string, keep the single quote as-is
			result.WriteByte(char)
		} else {
			result.WriteByte(char)
		}
	}

	return result.String()
}

// looksLikeJSONStringStart checks if a single quote at position pos starts a JSON string
// Converts outer single quotes to double quotes, leaves internal apostrophes alone
// Example: {'name': 'Dinson's App'} → {"name": "Dinson's App"}
func looksLikeJSONStringStart(s string, pos int) bool {
	// Look at what comes before the quote
	before := ' '
	if pos > 0 {
		for i := pos - 1; i >= 0; i-- {
			if !isWhitespace(s[i]) {
				before = rune(s[i])
				break
			}
		}
	}

	// Check if preceded by JSON structure characters
	validBefore := before == '{' || before == ',' || before == ':' || before == '['
	if !validBefore {
		return false
	}

	// Find the closing quote by looking for ' followed by JSON structure chars
	closingPos := findClosingSingleQuoteByContext(s, pos)
	if closingPos < 0 {
		return false // No closing quote found
	}

	return true
}

// findClosingSingleQuoteByContext finds the closing single quote by looking for what comes after
// This handles internal apostrophes correctly: 'Dinson's App' - finds the last ' before , } ] :
func findClosingSingleQuoteByContext(s string, pos int) int {
	// Scan forward looking for single quotes followed by JSON delimiters
	escaped := false
	for i := pos + 1; i < len(s); i++ {
		if escaped {
			escaped = false
			continue
		}
		if s[i] == '\\' {
			escaped = true
			continue
		}
		if s[i] == '\'' {
			// Check what comes after this quote
			afterQuote := ' '
			for j := i + 1; j < len(s); j++ {
				if !isWhitespace(s[j]) {
					afterQuote = rune(s[j])
					break
				}
			}
			// If followed by JSON delimiter, this is the closing quote
			if afterQuote == ':' || afterQuote == ',' || afterQuote == '}' || afterQuote == ']' {
				return i
			}
			// Otherwise, it's an internal apostrophe, keep looking
		}
	}
	return -1
}

// removeTrailingCommasStringAware removes trailing commas ONLY outside strings
func removeTrailingCommasStringAware(s string) string {
	var result strings.Builder
	result.Grow(len(s))

	inString := false
	escaped := false

	for i := 0; i < len(s); i++ {
		char := s[i]

		if escaped {
			result.WriteByte(char)
			escaped = false
			continue
		}

		if char == '\\' {
			result.WriteByte(char)
			escaped = true
			continue
		}

		if char == '"' {
			inString = !inString
			result.WriteByte(char)
			continue
		}

		if inString {
			result.WriteByte(char)
			continue
		}

		// Outside strings: check if this is a trailing comma
		if char == ',' {
			// Look ahead for optional whitespace followed by } or ]
			foundClosing := false
			j := i + 1
			for j < len(s) && isWhitespace(s[j]) {
				j++
			}
			if j < len(s) && (s[j] == '}' || s[j] == ']') {
				// This is a trailing comma - skip it
				foundClosing = true
			}

			if !foundClosing {
				// Not a trailing comma, keep it
				result.WriteByte(char)
			}
			// If it was trailing, we've skipped it
		} else {
			result.WriteByte(char)
		}
	}

	return result.String()
}

// balanceBrackets balances unmatched brackets and braces
// Also attempts to fix mismatched bracket types (e.g., } closing a [)
// Returns the repaired string and a list of warnings for coerced brackets
func balanceBrackets(s string) (string, []string) {
	var result strings.Builder
	result.Grow(len(s) + 10) // Extra space for potential additions

	var stack []rune
	var warnings []string
	inString := false
	escaped := false

	// Process character by character
	for _, char := range s {
		if escaped {
			result.WriteRune(char)
			escaped = false
			continue
		}

		if char == '\\' {
			result.WriteRune(char)
			escaped = true
			continue
		}

		if char == '"' {
			inString = !inString
			result.WriteRune(char)
			continue
		}

		if inString {
			result.WriteRune(char)
			continue
		}

		switch char {
		case '{', '[':
			stack = append(stack, char)
			result.WriteRune(char)
		case '}':
			if len(stack) > 0 {
				if stack[len(stack)-1] == '{' {
					// Correct match
					stack = stack[:len(stack)-1]
					result.WriteRune(char)
				} else if stack[len(stack)-1] == '[' {
					// Mismatched: } trying to close [
					// Convert to ] and pop
					stack = stack[:len(stack)-1]
					result.WriteRune(']')
					warnings = append(warnings, "coerced bracket type: } → ]")
				}
			} else {
				// Extra closer with no opener - skip it
				// (don't write it to result)
			}
		case ']':
			if len(stack) > 0 {
				if stack[len(stack)-1] == '[' {
					// Correct match
					stack = stack[:len(stack)-1]
					result.WriteRune(char)
				} else if stack[len(stack)-1] == '{' {
					// Mismatched: ] trying to close {
					// Convert to } and pop
					stack = stack[:len(stack)-1]
					result.WriteRune('}')
					warnings = append(warnings, "coerced bracket type: ] → }")
				}
			} else {
				// Extra closer with no opener - skip it
			}
		default:
			result.WriteRune(char)
		}
	}

	s = result.String()

	// If we're still in a string, close it
	if inString {
		s += "\""
	}

	// Append missing closing brackets in reverse order
	for i := len(stack) - 1; i >= 0; i-- {
		switch stack[i] {
		case '{':
			s += "}"
		case '[':
			s += "]"
		}
	}

	return s, warnings
}

func isWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// quoteUnquotedKeys adds double quotes around unquoted object keys
// Example: {name: "John"} → {"name": "John"}
func quoteUnquotedKeys(s string) string {
	var result strings.Builder
	result.Grow(len(s) + 20)

	inString := false
	escaped := false

	for i := 0; i < len(s); i++ {
		char := s[i]

		if escaped {
			result.WriteByte(char)
			escaped = false
			continue
		}

		if char == '\\' {
			result.WriteByte(char)
			escaped = true
			continue
		}

		if char == '"' {
			inString = !inString
			result.WriteByte(char)
			continue
		}

		if inString {
			result.WriteByte(char)
			continue
		}

		// Outside strings: look for unquoted keys (identifier followed by colon)
		if isIdentifierStart(char) {
			// Check if this looks like an unquoted key
			keyStart := i
			keyEnd := i

			// Scan the identifier
			for keyEnd < len(s) && isIdentifierChar(s[keyEnd]) {
				keyEnd++
			}

			// Skip whitespace after the identifier
			j := keyEnd
			for j < len(s) && isWhitespace(s[j]) {
				j++
			}

			// If followed by ':', this is an unquoted key
			if j < len(s) && s[j] == ':' {
				// Check if we're in a valid context (after { or ,)
				if isValidKeyContext(s, keyStart) {
					// Add quotes around the key
					result.WriteByte('"')
					result.WriteString(s[keyStart:keyEnd])
					result.WriteByte('"')
					// Add any whitespace between key and colon
					result.WriteString(s[keyEnd:j])
					i = j - 1 // -1 because loop will increment
					continue
				}
			}

			// Not an unquoted key, write as-is
			result.WriteByte(char)
		} else {
			result.WriteByte(char)
		}
	}

	return result.String()
}

func isIdentifierStart(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_' || b == '$'
}

func isValidKeyContext(s string, pos int) bool {
	// Look backwards to find the previous non-whitespace character
	for i := pos - 1; i >= 0; i-- {
		if !isWhitespace(s[i]) {
			return s[i] == '{' || s[i] == ','
		}
	}
	return false
}

// cleanErroneousBracketsInStrings removes trailing '}' or ']' preceded by single quotes
// from inside string values. These are often artifacts of malformed input.
// Example: "www'}" → "www"
func cleanErroneousBracketsInStrings(s string) string {
	var result strings.Builder
	result.Grow(len(s))

	inString := false
	escaped := false
	var stringBuffer strings.Builder

	for i := 0; i < len(s); i++ {
		char := s[i]

		if escaped {
			if inString {
				stringBuffer.WriteByte(char)
			} else {
				result.WriteByte(char)
			}
			escaped = false
			continue
		}

		if char == '\\' {
			if inString {
				stringBuffer.WriteByte(char)
			} else {
				result.WriteByte(char)
			}
			escaped = true
			continue
		}

		if char == '"' {
			if inString {
				// End of string - check if we need to clean it
				stringContent := stringBuffer.String()
				cleaned := cleanTrailingBrackets(stringContent)
				result.WriteString(cleaned)
				result.WriteByte(char)
				stringBuffer.Reset()
				inString = false
			} else {
				// Start of string
				result.WriteByte(char)
				inString = true
			}
			continue
		}

		if inString {
			stringBuffer.WriteByte(char)
		} else {
			result.WriteByte(char)
		}
	}

	// Handle unclosed string
	if inString {
		stringContent := stringBuffer.String()
		cleaned := cleanTrailingBrackets(stringContent)
		result.WriteString(cleaned)
	}

	return result.String()
}

func cleanTrailingBrackets(s string) string {
	// Remove trailing patterns like '}, '], '}, "]
	for {
		changed := false
		// Remove '} or ']
		if len(s) >= 2 {
			if (s[len(s)-2] == '\'' && s[len(s)-1] == '}') ||
				(s[len(s)-2] == '\'' && s[len(s)-1] == ']') {
				s = s[:len(s)-2]
				changed = true
			}
		}
		// Remove standalone ' at the end
		if len(s) >= 1 && s[len(s)-1] == '\'' {
			s = s[:len(s)-1]
			changed = true
		}
		if !changed {
			break
		}
	}
	return s
}

// ===========================
// Layer 3: Validation
// ===========================

// isValidJSON checks if a string is valid JSON
func isValidJSON(s string) bool {
	var parsed interface{}
	return json.Unmarshal([]byte(s), &parsed) == nil
}

// minifyJSON re-marshals JSON to remove whitespace and normalize formatting
func minifyJSON(s string) string {
	var parsed interface{}
	err := json.Unmarshal([]byte(s), &parsed)
	if err != nil {
		// If unmarshal fails, return original
		return s
	}

	minified, err := json.Marshal(parsed)
	if err != nil {
		// If marshal fails, return original
		return s
	}

	return string(minified)
}
