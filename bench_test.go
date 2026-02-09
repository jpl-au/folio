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
		decompress(compressed) //nolint:errcheck
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

func benchMissDB(b *testing.B, bloom bool) *DB {
	b.Helper()
	dir := b.TempDir()
	db, err := Open(dir, "bench.folio", Config{BloomFilter: bloom})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })

	for i := 0; i < 1000; i++ {
		db.Set("doc"+strconv.Itoa(i), "content")
	}
	return db
}

func BenchmarkGetMissBloom(b *testing.B) {
	db := benchMissDB(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("miss-" + strconv.Itoa(i))
	}
}

func BenchmarkGetMissNoBloom(b *testing.B) {
	db := benchMissDB(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("miss-" + strconv.Itoa(i))
	}
}

func BenchmarkExistsMissBloom(b *testing.B) {
	db := benchMissDB(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exists("miss-" + strconv.Itoa(i))
	}
}

func BenchmarkExistsMissNoBloom(b *testing.B) {
	db := benchMissDB(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exists("miss-" + strconv.Itoa(i))
	}
}

func BenchmarkBloomAdd(b *testing.B) {
	bl := newBloom()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bl.Add("id-" + strconv.Itoa(i))
	}
}

func BenchmarkBloomContains(b *testing.B) {
	bl := newBloom()
	for i := 0; i < 1000; i++ {
		bl.Add("id-" + strconv.Itoa(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bl.Contains("miss-" + strconv.Itoa(i))
	}
}

// benchMixedDB creates a DB with 500 docs in the sorted index and 500 in sparse.
func benchMixedDB(b *testing.B, bloom bool) *DB {
	b.Helper()
	dir := b.TempDir()
	db, err := Open(dir, "bench.folio", Config{BloomFilter: bloom})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })

	// 500 docs compacted into sorted index
	for i := 0; i < 500; i++ {
		db.Set("sorted-"+strconv.Itoa(i), "content")
	}
	db.Compact()

	// 500 docs in sparse region
	for i := 0; i < 500; i++ {
		db.Set("sparse-"+strconv.Itoa(i), "content")
	}
	return db
}

func BenchmarkGetHitSortedBloom(b *testing.B) {
	db := benchMixedDB(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("sorted-" + strconv.Itoa(i%500))
	}
}

func BenchmarkGetHitSortedNoBloom(b *testing.B) {
	db := benchMixedDB(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("sorted-" + strconv.Itoa(i%500))
	}
}

func BenchmarkGetHitSparseBloom(b *testing.B) {
	db := benchMixedDB(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("sparse-" + strconv.Itoa(i%500))
	}
}

func BenchmarkGetHitSparseNoBloom(b *testing.B) {
	db := benchMixedDB(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("sparse-" + strconv.Itoa(i%500))
	}
}

func BenchmarkGetMissMixedBloom(b *testing.B) {
	db := benchMixedDB(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("miss-" + strconv.Itoa(i))
	}
}

func BenchmarkGetMissMixedNoBloom(b *testing.B) {
	db := benchMixedDB(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("miss-" + strconv.Itoa(i))
	}
}

// Mixed workload: 1/3 sorted hits, 1/3 sparse hits, 1/3 misses.
func BenchmarkGetMixedWorkloadBloom(b *testing.B) {
	db := benchMixedDB(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 3 {
		case 0:
			db.Get("sorted-" + strconv.Itoa(i%500))
		case 1:
			db.Get("sparse-" + strconv.Itoa(i%500))
		case 2:
			db.Get("miss-" + strconv.Itoa(i))
		}
	}
}

func BenchmarkGetMixedWorkloadNoBloom(b *testing.B) {
	db := benchMixedDB(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 3 {
		case 0:
			db.Get("sorted-" + strconv.Itoa(i%500))
		case 1:
			db.Get("sparse-" + strconv.Itoa(i%500))
		case 2:
			db.Get("miss-" + strconv.Itoa(i))
		}
	}
}

func BenchmarkOpenBloom(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir, "bench.folio", Config{})
	for i := 0; i < 1000; i++ {
		db.Set("doc"+strconv.Itoa(i), "content")
	}
	db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, _ := Open(dir, "bench.folio", Config{BloomFilter: true})
		db.Close()
	}
}

func BenchmarkOpenNoBloom(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir, "bench.folio", Config{})
	for i := 0; i < 1000; i++ {
		db.Set("doc"+strconv.Itoa(i), "content")
	}
	db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, _ := Open(dir, "bench.folio", Config{BloomFilter: false})
		db.Close()
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
