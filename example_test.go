package folio_test

import (
	"fmt"
	"log"
	"os"

	"github.com/jpl-au/folio"
)

func Example() {
	dir, _ := os.MkdirTemp("", "folio-example")
	defer os.RemoveAll(dir)

	// Open or create a database
	db, err := folio.Open(dir, "myapp.folio", folio.Config{})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Store a document
	db.Set("readme", "# My App\n\nWelcome to my application.")

	// Retrieve it
	content, _ := db.Get("readme")
	fmt.Println(content)
	// Output: # My App
	//
	// Welcome to my application.
}

func ExampleDB_Set() {
	dir, _ := os.MkdirTemp("", "folio-example")
	defer os.RemoveAll(dir)

	db, _ := folio.Open(dir, "example.folio", folio.Config{})
	defer db.Close()

	// Create a new document
	err := db.Set("config", "theme: dark\nlanguage: en")
	if err != nil {
		log.Fatal(err)
	}

	// Update overwrites the previous version (history preserved)
	db.Set("config", "theme: light\nlanguage: en")
}

func ExampleDB_Get() {
	dir, _ := os.MkdirTemp("", "folio-example")
	defer os.RemoveAll(dir)

	db, _ := folio.Open(dir, "example.folio", folio.Config{})
	defer db.Close()

	db.Set("greeting", "Hello, World!")

	content, err := db.Get("greeting")
	if err == folio.ErrNotFound {
		fmt.Println("Document not found")
		return
	}
	fmt.Println(content)
	// Output: Hello, World!
}

func ExampleDB_History() {
	dir, _ := os.MkdirTemp("", "folio-example")
	defer os.RemoveAll(dir)

	db, _ := folio.Open(dir, "example.folio", folio.Config{})
	defer db.Close()

	// Create multiple versions
	db.Set("doc", "Version 1")
	db.Set("doc", "Version 2")
	db.Set("doc", "Version 3")

	// Retrieve all versions (oldest first)
	versions, _ := db.History("doc")
	for i, v := range versions {
		fmt.Printf("v%d: %s\n", i+1, v.Data)
	}
	// Output: v1: Version 1
	// v2: Version 2
	// v3: Version 3
}

func ExampleDB_List() {
	dir, _ := os.MkdirTemp("", "folio-example")
	defer os.RemoveAll(dir)

	db, _ := folio.Open(dir, "example.folio", folio.Config{})
	defer db.Close()

	db.Set("apple", "A fruit")
	db.Set("banana", "Another fruit")
	db.Set("carrot", "A vegetable")

	labels, _ := db.List()
	fmt.Printf("Documents: %d\n", len(labels))
	// Output: Documents: 3
}

func ExampleDB_Delete() {
	dir, _ := os.MkdirTemp("", "folio-example")
	defer os.RemoveAll(dir)

	db, _ := folio.Open(dir, "example.folio", folio.Config{})
	defer db.Close()

	db.Set("temp", "Temporary data")

	// Delete removes from active documents but preserves history
	db.Delete("temp")

	_, err := db.Get("temp")
	fmt.Println(err == folio.ErrNotFound)
	// Output: true
}

func ExampleDB_Compact() {
	dir, _ := os.MkdirTemp("", "folio-example")
	defer os.RemoveAll(dir)

	db, _ := folio.Open(dir, "example.folio", folio.Config{})
	defer db.Close()

	// After many writes, compact reorganises for faster reads
	for i := 0; i < 100; i++ {
		db.Set("counter", fmt.Sprintf("%d", i))
	}

	// Compact sorts data for binary search (preserves history)
	db.Compact()

	// Purge removes history, keeping only current versions
	db.Purge()
}

func ExampleDB_Search() {
	dir, _ := os.MkdirTemp("", "folio-example")
	defer os.RemoveAll(dir)

	db, _ := folio.Open(dir, "example.folio", folio.Config{})
	defer db.Close()

	db.Set("readme", "# Welcome\n\nThis is the README file.")
	db.Set("changelog", "# Changelog\n\n## v1.0\n- Initial release")

	// Search file content with regex
	matches, _ := db.Search("README", folio.SearchOptions{})
	fmt.Printf("Matches: %d\n", len(matches))
}

func ExampleConfig() {
	dir, _ := os.MkdirTemp("", "folio-example")
	defer os.RemoveAll(dir)

	// Custom configuration
	cfg := folio.Config{
		HashAlgorithm: folio.AlgXXHash3, // Default, fastest
		SyncWrites:    true,              // fsync after each write
		ReadBuffer:    128 * 1024,        // 128KB read buffer
		MaxRecordSize: 32 * 1024 * 1024,  // 32MB max record
	}

	db, _ := folio.Open(dir, "custom.folio", cfg)
	defer db.Close()
}
