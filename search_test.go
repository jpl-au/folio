// Search and MatchLabel tests.
//
// Search scans every record in the file and matches a pattern against
// the document content. MatchLabel does the same but matches against
// the label field. Both support regex patterns and have two execution
// paths: a literal fast path (when the pattern has no regex meta-
// characters) that matches against the raw JSON bytes without
// unescaping, and a regex path that optionally unescapes content first
// (Decode: true).
//
// The fast path is important for performance — it avoids allocating
// a new string for every record. But it means special characters in
// content (quotes, backslashes, newlines) are stored as JSON escape
// sequences (\" \\ \n) in the raw bytes. These tests verify that both
// paths produce correct results for all combinations of: plain text,
// quoted content, content with newlines, content with backslashes,
// case-sensitive/insensitive matching, regex patterns, and the Decode
// option.
package folio

import (
	"testing"
)

// TestSearchMatchFound verifies the basic case: a substring match in
// document content. If Search failed to scan the sparse region or
// miscompared the pattern, it would return empty results for content
// that clearly matches.
func TestSearchMatchFound(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "hello world")

	matches, err := collect(db.Search("hello", SearchOptions{}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(matches) == 0 {
		t.Error("expected at least one match")
	}
}

// TestSearchNoMatch verifies that Search returns empty when no document
// contains the pattern. If Search had a bug that matched empty strings
// or partial bytes, every query would return false positives.
func TestSearchNoMatch(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "hello world")

	matches, _ := collect(db.Search("xyz", SearchOptions{}))
	if len(matches) != 0 {
		t.Errorf("expected no matches, got %d", len(matches))
	}
}

// TestSearchCaseInsensitiveDefault verifies that Search is case-
// insensitive by default. Users expect "hello" to match "Hello" unless
// they explicitly opt into case-sensitive mode. If the default were
// case-sensitive, users would miss documents that differ only in case.
func TestSearchCaseInsensitiveDefault(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "Hello World")

	matches, _ := collect(db.Search("HELLO", SearchOptions{}))
	if len(matches) == 0 {
		t.Error("case insensitive search should match")
	}
}

// TestSearchCaseSensitive verifies the CaseSensitive option. When
// enabled, "HELLO" must not match "Hello". If the option were ignored,
// users who need exact matching (e.g. code search) would get spurious
// results.
func TestSearchCaseSensitive(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "Hello World")

	matches, _ := collect(db.Search("HELLO", SearchOptions{CaseSensitive: true}))
	if len(matches) != 0 {
		t.Error("case sensitive search should not match")
	}

	matches, _ = collect(db.Search("Hello", SearchOptions{CaseSensitive: true}))
	if len(matches) == 0 {
		t.Error("case sensitive search should match exact case")
	}
}

// TestSearchEarlyBreak verifies that breaking out of the range loop
// stops the scan without consuming all results. The caller controls
// result count by breaking — no Limit option needed.
func TestSearchEarlyBreak(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "abc")
	db.Set("b", "abc")
	db.Set("c", "abc")

	var count int
	for _, err := range db.Search("abc", SearchOptions{}) {
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		count++
		if count >= 2 {
			break
		}
	}
	if count != 2 {
		t.Errorf("expected 2 results before break, got %d", count)
	}
}

// TestSearchRegex verifies that patterns with regex metacharacters are
// compiled and matched as regular expressions. The literal fast path
// would miss "hel.*rld" because it doesn't interpret metacharacters.
func TestSearchRegex(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "hello world")

	matches, _ := collect(db.Search("hel.*rld", SearchOptions{}))
	if len(matches) == 0 {
		t.Error("regex should match")
	}
}

// TestSearchInvalidRegex verifies that an invalid regex pattern returns
// ErrInvalidPattern rather than panicking. If Search passed the pattern
// to regexp.Compile without checking, an invalid pattern would cause a
// runtime panic that crashes the caller.
func TestSearchInvalidRegex(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")

	_, err := collect(db.Search("[invalid", SearchOptions{}))
	if err != ErrInvalidPattern {
		t.Errorf("expected ErrInvalidPattern, got %v", err)
	}
}

// TestSearchClosed verifies that Search on a closed database returns
// ErrClosed. Search reads from the file handle; without this check, it
// would attempt to read from a closed handle and produce an OS error.
func TestSearchClosed(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Close()

	_, err := collect(db.Search("content", SearchOptions{}))
	if err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

// TestMatchLabelFound verifies that MatchLabel finds a document whose
// label contains the pattern. MatchLabel is used for fuzzy label lookup
// (e.g. "find all documents whose label contains 'app'"). If it only
// matched exact labels, users would need to know the full label.
func TestMatchLabelFound(t *testing.T) {
	db := openTestDB(t)

	db.Set("my-app", "content")

	matches, err := collect(db.MatchLabel("app"))
	if err != nil {
		t.Fatalf("MatchLabel: %v", err)
	}
	if len(matches) == 0 {
		t.Error("expected at least one match")
	}
	if matches[0].Label != "my-app" {
		t.Errorf("Label = %q, want %q", matches[0].Label, "my-app")
	}
}

// TestMatchLabelNoMatch verifies that MatchLabel returns empty when no
// label contains the pattern.
func TestMatchLabelNoMatch(t *testing.T) {
	db := openTestDB(t)

	db.Set("my-app", "content")

	matches, _ := collect(db.MatchLabel("xyz"))
	if len(matches) != 0 {
		t.Errorf("expected no matches, got %d", len(matches))
	}
}

// TestMatchLabelCaseInsensitive verifies that MatchLabel is case-
// insensitive by default, matching the same convention as Search.
func TestMatchLabelCaseInsensitive(t *testing.T) {
	db := openTestDB(t)

	db.Set("MyApp", "content")

	matches, _ := collect(db.MatchLabel("myapp"))
	if len(matches) == 0 {
		t.Error("case insensitive match should work")
	}
}

// TestMatchLabelInvalidRegex verifies that an invalid regex pattern
// returns ErrInvalidPattern.
func TestMatchLabelInvalidRegex(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")

	_, err := collect(db.MatchLabel("(?P<invalid"))
	if err != ErrInvalidPattern {
		t.Errorf("expected ErrInvalidPattern, got %v", err)
	}
}

// TestMatchLabelClosed verifies that MatchLabel returns ErrClosed on a
// closed database.
func TestMatchLabelClosed(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Close()

	_, err := collect(db.MatchLabel("doc"))
	if err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

// TestMatchLabelMultiple verifies that MatchLabel returns all matching
// documents, not just the first. If it stopped after the first match,
// users couldn't discover all documents in a namespace (e.g. all
// "app-*" documents).
func TestMatchLabelMultiple(t *testing.T) {
	db := openTestDB(t)

	db.Set("app-one", "content1")
	db.Set("app-two", "content2")
	db.Set("other", "content3")

	matches, _ := collect(db.MatchLabel("app"))
	if len(matches) != 2 {
		t.Errorf("expected 2 matches, got %d", len(matches))
	}
}

// TestSearchDecodeQuotes exercises both execution paths for content
// containing double quotes. In raw JSON, a quote in content is stored
// as \". The literal fast path JSON-escapes the query to match the raw
// bytes; the Decode path unescapes the content first. Both must find
// the match — if either path mishandled quote escaping, documents with
// quoted content would be unsearchable.
func TestSearchDecodeQuotes(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", `hello "world"`)

	// With Decode, the unescaped quotes should be searchable.
	matches, err := collect(db.Search(`"world"`, SearchOptions{Decode: true}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(matches) == 0 {
		t.Error("decoded search should match quoted content")
	}

	// Without Decode, the literal fast path JSON-escapes the query
	// so it matches the raw \" sequences directly.
	matches, _ = collect(db.Search(`"world"`, SearchOptions{}))
	if len(matches) == 0 {
		t.Error("literal search should match quoted content via escaped query")
	}
}

// TestSearchRegexDecodeQuotes verifies that the regex path without
// Decode cannot match literal quotes (because they're escaped in the
// raw JSON), but with Decode it can. This is the key difference between
// the two modes: without Decode, the regex runs against raw JSON bytes
// where quotes are \"; with Decode, it runs against the unescaped
// content where quotes are literal ".
func TestSearchRegexDecodeQuotes(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", `hello "world"`)

	// Regex path (pattern has metacharacter) without Decode cannot match
	// literal quotes because the raw JSON has escaped quotes (\").
	matches, _ := collect(db.Search(`"wor.d"`, SearchOptions{}))
	if len(matches) != 0 {
		t.Error("regex on raw JSON should not match literal quotes")
	}

	// With Decode, the unescaped content is matched.
	matches, _ = collect(db.Search(`"wor.d"`, SearchOptions{Decode: true}))
	if len(matches) == 0 {
		t.Error("regex with decode should match quoted content")
	}
}

// TestSearchLiteralNewline verifies that the literal fast path matches
// content containing newlines. In JSON, a newline in content is stored
// as \n (two bytes). The literal path JSON-escapes the query's newline
// to match the raw bytes. If it didn't escape the query, the \n in the
// raw bytes would never match the literal newline character.
func TestSearchLiteralNewline(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "line1\nline2")

	// Literal path: the newline in the pattern is JSON-escaped to \n
	// and matched against the raw JSON bytes without unescaping content.
	matches, err := collect(db.Search("line1\nline2", SearchOptions{}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(matches) == 0 {
		t.Error("literal search should match content with newlines")
	}
}

// TestSearchBothPathsNewline verifies that both the literal and regex
// paths find a substring in content that spans a newline. This catches
// bugs where the search might stop at the first line of multi-line
// content.
func TestSearchBothPathsNewline(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "line1\nline2")

	// Literal path: "line1" has no metacharacters.
	matches, _ := collect(db.Search("line1", SearchOptions{}))
	if len(matches) == 0 {
		t.Error("literal path should match substring in newline content")
	}

	// Regex path: "line." has a metacharacter.
	matches, _ = collect(db.Search("line.", SearchOptions{}))
	if len(matches) == 0 {
		t.Error("regex path should match pattern in newline content")
	}
}

// TestSearchBothPathsQuotes verifies that both paths find a quoted
// substring. The literal path JSON-escapes the query; the regex+Decode
// path unescapes the content. Both must produce a match.
func TestSearchBothPathsQuotes(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", `say "hello" please`)

	// Literal path: `"hello"` has no regex metacharacters (quote is not one).
	matches, _ := collect(db.Search(`"hello"`, SearchOptions{}))
	if len(matches) == 0 {
		t.Error("literal path should match quoted substring")
	}

	// Regex path: `"hel.o"` has a metacharacter.
	matches, _ = collect(db.Search(`"hel.o"`, SearchOptions{Decode: true}))
	if len(matches) == 0 {
		t.Error("regex path with decode should match quoted content")
	}
}

// TestSearchDecodePlain verifies that Decode:true works with plain
// content that has no escape sequences. The decode path must not break
// content that doesn't need unescaping — if it corrupted plain strings,
// the most common search case would fail.
func TestSearchDecodePlain(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "hello world")

	// Decode on plain content (fast path, no escapes) should still match.
	matches, err := collect(db.Search("hello", SearchOptions{Decode: true}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(matches) == 0 {
		t.Error("decoded search should match plain content")
	}
}

// TestSearchDecodeBackslash verifies that the Decode path correctly
// unescapes backslashes. In JSON, a literal backslash is stored as \\.
// The regex `path\\to` matches the unescaped content "path\to". If
// unescape() didn't handle \\, the decoded content would still contain
// the escape sequence and the regex wouldn't match.
func TestSearchDecodeBackslash(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", `path\to\file`)

	matches, err := collect(db.Search(`path\\to`, SearchOptions{Decode: true}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(matches) == 0 {
		t.Error("decoded search should match backslash content")
	}
}

// TestSearchDecodeNewline verifies that the Decode path correctly
// unescapes newlines. In JSON, a newline is stored as \n (two bytes).
// The Decode path must convert this to a real newline character so the
// search pattern (which contains a real newline) can match.
func TestSearchDecodeNewline(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "line1\nline2")

	// Search for the literal newline character in decoded content.
	matches, err := collect(db.Search("line1\nline2", SearchOptions{Decode: true}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(matches) == 0 {
		t.Error("decoded search should match newline content")
	}
}
