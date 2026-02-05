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
