package folio

import (
	"strconv"
	"strings"
	"testing"
)

func BenchmarkSet(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir, "bench.folio", Config{})
	defer db.Close()

	content := strings.Repeat("x", 1024) // 1KB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Set("doc"+strconv.Itoa(i), content)
	}
}

func BenchmarkSetSameKey(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir, "bench.folio", Config{})
	defer db.Close()

	content := strings.Repeat("x", 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Set("doc", content)
	}
}

func BenchmarkGetSparse(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir, "bench.folio", Config{})
	defer db.Close()

	db.Set("doc", "content")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("doc")
	}
}

func BenchmarkGetSorted(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir, "bench.folio", Config{})
	defer db.Close()

	db.Set("doc", "content")
	db.Compact()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("doc")
	}
}

func BenchmarkGetManyDocsSparse(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir, "bench.folio", Config{})
	defer db.Close()

	for i := 0; i < 1000; i++ {
		db.Set("doc"+strconv.Itoa(i), "content")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("doc" + strconv.Itoa(i%1000))
	}
}

func BenchmarkGetManyDocsSorted(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir, "bench.folio", Config{})
	defer db.Close()

	for i := 0; i < 1000; i++ {
		db.Set("doc"+strconv.Itoa(i), "content")
	}
	db.Compact()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("doc" + strconv.Itoa(i%1000))
	}
}

func BenchmarkExists(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir, "bench.folio", Config{})
	defer db.Close()

	db.Set("doc", "content")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exists("doc")
	}
}

func BenchmarkList(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir, "bench.folio", Config{})
	defer db.Close()

	for i := 0; i < 100; i++ {
		db.Set("doc"+strconv.Itoa(i), "content")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.List()
	}
}

func BenchmarkHistory(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir, "bench.folio", Config{})
	defer db.Close()

	for i := 0; i < 10; i++ {
		db.Set("doc", "version"+strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.History("doc")
	}
}

func BenchmarkCompact(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		db, _ := Open(dir, "bench.folio", Config{})
		for j := 0; j < 100; j++ {
			db.Set("doc"+strconv.Itoa(j), "content")
		}
		b.StartTimer()

		db.Compact()

		b.StopTimer()
		db.Close()
	}
}

func BenchmarkHashXXHash3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hash("test-label", AlgXXHash3)
	}
}

func BenchmarkHashFNV1a(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hash("test-label", AlgFNV1a)
	}
}

func BenchmarkHashBlake2b(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hash("test-label", AlgBlake2b)
	}
}

func BenchmarkCompress1KB(b *testing.B) {
	data := []byte(strings.Repeat("# Heading\n\nSome markdown content.\n\n", 30))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compress(data)
	}
}

func BenchmarkCompress50KB(b *testing.B) {
	data := []byte(strings.Repeat("# Heading\n\nSome markdown content.\n\n", 1500))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compress(data)
	}
}

func BenchmarkDecompress1KB(b *testing.B) {
	data := []byte(strings.Repeat("# Heading\n\nSome markdown content.\n\n", 30))
	compressed := compress(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decompress(compressed)
	}
}

func benchSearchDB(b *testing.B) *DB {
	b.Helper()
	dir := b.TempDir()
	db, err := Open(dir, "bench.folio", Config{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })

	for i := 0; i < 100; i++ {
		db.Set("doc"+strconv.Itoa(i),
			`hello "world" with some\nescaped content `+strconv.Itoa(i)+
				` and more text to make it roughly one kilobyte `+
				strings.Repeat("padding ", 100))
	}
	return db
}

func BenchmarkSearchRaw(b *testing.B) {
	db := benchSearchDB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Search("hello", SearchOptions{})
	}
}

func BenchmarkSearchDecode(b *testing.B) {
	db := benchSearchDB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Search("hello", SearchOptions{Decode: true})
	}
}

func BenchmarkSearchRawMiss(b *testing.B) {
	db := benchSearchDB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Search("zzznomatch", SearchOptions{})
	}
}

func BenchmarkSearchDecodeMiss(b *testing.B) {
	db := benchSearchDB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Search("zzznomatch", SearchOptions{Decode: true})
	}
}

func BenchmarkMatchLabel(b *testing.B) {
	db := benchSearchDB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.MatchLabel("doc")
	}
}

func BenchmarkMatchLabelMiss(b *testing.B) {
	db := benchSearchDB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.MatchLabel("zzznomatch")
	}
}

func BenchmarkUnescape(b *testing.B) {
	data := []byte(`hello \"world\" path\\to\\file line1\nline2`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		unescape(data)
	}
}

func BenchmarkUnescapeClean(b *testing.B) {
	data := []byte("hello world no escapes here at all")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		unescape(data)
	}
}

func BenchmarkRehash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		db, _ := Open(dir, "bench.folio", Config{})
		for j := 0; j < 100; j++ {
			db.Set("doc"+strconv.Itoa(j), "content")
		}
		b.StartTimer()

		db.Rehash(AlgFNV1a)

		b.StopTimer()
		db.Close()
	}
}
