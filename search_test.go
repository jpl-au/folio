package folio

import (
	"testing"
)

func TestSearchMatchFound(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "hello world")

	matches, err := db.Search("hello", SearchOptions{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(matches) == 0 {
		t.Error("expected at least one match")
	}
}

func TestSearchNoMatch(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "hello world")

	matches, _ := db.Search("xyz", SearchOptions{})
	if len(matches) != 0 {
		t.Errorf("expected no matches, got %d", len(matches))
	}
}

func TestSearchCaseInsensitiveDefault(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "Hello World")

	matches, _ := db.Search("HELLO", SearchOptions{})
	if len(matches) == 0 {
		t.Error("case insensitive search should match")
	}
}

func TestSearchCaseSensitive(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "Hello World")

	matches, _ := db.Search("HELLO", SearchOptions{CaseSensitive: true})
	if len(matches) != 0 {
		t.Error("case sensitive search should not match")
	}

	matches, _ = db.Search("Hello", SearchOptions{CaseSensitive: true})
	if len(matches) == 0 {
		t.Error("case sensitive search should match exact case")
	}
}

func TestSearchLimit(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "abc abc abc abc abc")

	matches, _ := db.Search("abc", SearchOptions{Limit: 2})
	if len(matches) > 2 {
		t.Errorf("limit not respected: got %d matches", len(matches))
	}
}

func TestSearchRegex(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "hello world")

	matches, _ := db.Search("hel.*rld", SearchOptions{})
	if len(matches) == 0 {
		t.Error("regex should match")
	}
}

func TestSearchInvalidRegex(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")

	_, err := db.Search("[invalid", SearchOptions{})
	if err != ErrInvalidPattern {
		t.Errorf("expected ErrInvalidPattern, got %v", err)
	}
}

func TestSearchClosed(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Close()

	_, err := db.Search("content", SearchOptions{})
	if err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestMatchLabelFound(t *testing.T) {
	db := openTestDB(t)

	db.Set("my-app", "content")

	matches, err := db.MatchLabel("app")
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

func TestMatchLabelNoMatch(t *testing.T) {
	db := openTestDB(t)

	db.Set("my-app", "content")

	matches, _ := db.MatchLabel("xyz")
	if len(matches) != 0 {
		t.Errorf("expected no matches, got %d", len(matches))
	}
}

func TestMatchLabelCaseInsensitive(t *testing.T) {
	db := openTestDB(t)

	db.Set("MyApp", "content")

	matches, _ := db.MatchLabel("myapp")
	if len(matches) == 0 {
		t.Error("case insensitive match should work")
	}
}

func TestMatchLabelInvalidRegex(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")

	_, err := db.MatchLabel("(?P<invalid")
	if err != ErrInvalidPattern {
		t.Errorf("expected ErrInvalidPattern, got %v", err)
	}
}

func TestMatchLabelClosed(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Close()

	_, err := db.MatchLabel("doc")
	if err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestMatchLabelMultiple(t *testing.T) {
	db := openTestDB(t)

	db.Set("app-one", "content1")
	db.Set("app-two", "content2")
	db.Set("other", "content3")

	matches, _ := db.MatchLabel("app")
	if len(matches) != 2 {
		t.Errorf("expected 2 matches, got %d", len(matches))
	}
}

func TestSearchDecodeQuotes(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", `hello "world"`)

	// With Decode, the unescaped quotes should be searchable.
	matches, err := db.Search(`"world"`, SearchOptions{Decode: true})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(matches) == 0 {
		t.Error("decoded search should match quoted content")
	}

	// Without Decode, the literal fast path JSON-escapes the query
	// so it matches the raw \" sequences directly.
	matches, _ = db.Search(`"world"`, SearchOptions{})
	if len(matches) == 0 {
		t.Error("literal search should match quoted content via escaped query")
	}
}

func TestSearchRegexDecodeQuotes(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", `hello "world"`)

	// Regex path (pattern has metacharacter) without Decode cannot match
	// literal quotes because the raw JSON has escaped quotes (\").
	matches, _ := db.Search(`"wor.d"`, SearchOptions{})
	if len(matches) != 0 {
		t.Error("regex on raw JSON should not match literal quotes")
	}

	// With Decode, the unescaped content is matched.
	matches, _ = db.Search(`"wor.d"`, SearchOptions{Decode: true})
	if len(matches) == 0 {
		t.Error("regex with decode should match quoted content")
	}
}

func TestSearchLiteralNewline(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "line1\nline2")

	// Literal path: the newline in the pattern is JSON-escaped to \n
	// and matched against the raw JSON bytes without unescaping content.
	matches, err := db.Search("line1\nline2", SearchOptions{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(matches) == 0 {
		t.Error("literal search should match content with newlines")
	}
}

func TestSearchBothPathsNewline(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "line1\nline2")

	// Literal path: "line1" has no metacharacters.
	matches, _ := db.Search("line1", SearchOptions{})
	if len(matches) == 0 {
		t.Error("literal path should match substring in newline content")
	}

	// Regex path: "line." has a metacharacter.
	matches, _ = db.Search("line.", SearchOptions{})
	if len(matches) == 0 {
		t.Error("regex path should match pattern in newline content")
	}
}

func TestSearchBothPathsQuotes(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", `say "hello" please`)

	// Literal path: `"hello"` has no regex metacharacters (quote is not one).
	matches, _ := db.Search(`"hello"`, SearchOptions{})
	if len(matches) == 0 {
		t.Error("literal path should match quoted substring")
	}

	// Regex path: `"hel.o"` has a metacharacter.
	matches, _ = db.Search(`"hel.o"`, SearchOptions{Decode: true})
	if len(matches) == 0 {
		t.Error("regex path with decode should match quoted content")
	}
}

func TestSearchDecodePlain(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "hello world")

	// Decode on plain content (fast path, no escapes) should still match.
	matches, err := db.Search("hello", SearchOptions{Decode: true})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(matches) == 0 {
		t.Error("decoded search should match plain content")
	}
}

func TestSearchDecodeBackslash(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", `path\to\file`)

	matches, err := db.Search(`path\\to`, SearchOptions{Decode: true})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(matches) == 0 {
		t.Error("decoded search should match backslash content")
	}
}

func TestSearchDecodeNewline(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "line1\nline2")

	// Search for the literal newline character in decoded content.
	matches, err := db.Search("line1\nline2", SearchOptions{Decode: true})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(matches) == 0 {
		t.Error("decoded search should match newline content")
	}
}
