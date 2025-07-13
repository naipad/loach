package loach

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/tidwall/gjson"
)

const (
	replyOK              = "ok"
	replyNotFound        = "leveldb: not found"
	replyError           = "error"
	scoreByteLen         = 8
	scoreMin      uint64 = 0
	scoreMax      uint64 = math.MaxUint64 // 18446744073709551615
)

var (
	HashPrefix      = []byte("H_") //H_
	ZsetPrefix      = []byte("Z_") //Z_
	ZsetKeyPrefix   = []byte("k")  //k
	ZsetScorePrefix = []byte("s")  //s
	SplitChar       = []byte("_")  //_
)

type (
	RawData []byte
	DB      struct {
		*leveldb.DB
	}
	Reply struct {
		State string
		Data  []RawData
	}
	Entry struct {
		Key, Value RawData
	}
)

type DBStats struct {
	Compactions      string                 `json:"compactions"`        // raw compaction stats text
	TableStats       map[int]TableStat      `json:"table_stats"`        // parsed per-level table stats
	SSTableInfo      string                 `json:"sstable_info"`       // raw sstables info text
	SSTables         map[int][]SSTableEntry `json:"sstables"`           // parsed per-level SSTable entries
	BlockPoolInfo    string                 `json:"block_pool_info"`    // raw block pool info
	OpenedTablesInfo string                 `json:"opened_tables_info"` // raw opened tables info
}

type TableStat struct {
	Tables int     `json:"tables"`
	SizeMB float64 `json:"size_mb"`
	Score  float64 `json:"score"`
}

type SSTableEntry struct {
	FileNumber int   `json:"file_number"`
	FileSize   int64 `json:"file_size"`
}

func Open(dbPath string, o *opt.Options) (*DB, error) {
	database, err := leveldb.OpenFile(dbPath, o)
	if err != nil {
		if errors.IsCorrupted(err) {
			if database, err = leveldb.RecoverFile(dbPath, o); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	return &DB{database}, nil
}

func OpenDefault(dbPath string) (*DB, error) {
	o := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}
	database, err := leveldb.OpenFile(dbPath, o)
	if err != nil {
		if errors.IsCorrupted(err) {
			if database, err = leveldb.RecoverFile(dbPath, o); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return &DB{database}, nil
}

func (db *DB) Compact() error {
	return db.CompactRange(util.Range{Start: nil, Limit: nil})
}

func (db *DB) StatsStruct() (*DBStats, error) {
	stats := &DBStats{
		TableStats: make(map[int]TableStat),
		SSTables:   make(map[int][]SSTableEntry),
	}

	// Parse leveldb.stats
	if text, err := db.DB.GetProperty("leveldb.stats"); err == nil {
		stats.Compactions = text

		lines := strings.Split(text, "\n")
		parse := false
		for _, line := range lines {
			if strings.HasPrefix(line, " Level |") {
				parse = true
				continue
			}
			if parse {
				// example line format:
				//    0   |        2 |       4.5 |       1.0
				var level, tables int
				var size, score float64
				n, _ := fmt.Sscanf(line, " %d | %d | %f | %f", &level, &tables, &size, &score)
				if n == 4 {
					stats.TableStats[level] = TableStat{
						Tables: tables,
						SizeMB: size,
						Score:  score,
					}
				}
			}
		}
	} else {
		return nil, err
	}

	// Parse leveldb.sstables
	if val, err := db.DB.GetProperty("leveldb.sstables"); err == nil {
		stats.SSTableInfo = val

		lines := strings.Split(val, "\n")
		currentLevel := -1
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "--- level ") {
				_, err := fmt.Sscanf(line, "--- level %d ---", &currentLevel)
				if err != nil {
					currentLevel = -1
				}
				continue
			}

			if currentLevel >= 0 && line != "" {
				var fileNumber int
				var fileSize int64
				if _, err := fmt.Sscanf(line, "%d:%d", &fileNumber, &fileSize); err == nil {
					stats.SSTables[currentLevel] = append(stats.SSTables[currentLevel], SSTableEntry{
						FileNumber: fileNumber,
						FileSize:   fileSize,
					})
				}
			}
		}
	} else {
		return nil, err
	}

	// Get block pool info
	if val, err := db.DB.GetProperty("leveldb.blockpool"); err == nil {
		stats.BlockPoolInfo = val
	}

	// Get opened tables info
	if val, err := db.DB.GetProperty("leveldb.openedtables"); err == nil {
		stats.OpenedTablesInfo = val
	}

	return stats, nil
}

func (db *DB) GetSnapshot(key []byte) ([]byte, error) {
	snap, err := db.DB.GetSnapshot()
	if err != nil {
		return nil, err
	}
	defer snap.Release()
	return snap.Get(key, nil)
}

func (db *DB) BackupTo(backupPath string) error {
	snapshot, err := db.DB.GetSnapshot()
	if err != nil {
		return err
	}
	defer snapshot.Release()

	backupDB, err := leveldb.OpenFile(backupPath, &opt.Options{ErrorIfExist: true})
	if err != nil {
		return err
	}
	defer backupDB.Close()

	iter := snapshot.NewIterator(nil, nil)
	defer iter.Release()

	batch := new(leveldb.Batch)
	count := 0
	for iter.Next() {
		key := append([]byte{}, iter.Key()...) // 拷贝 key/value 避免复用
		value := append([]byte{}, iter.Value()...)
		batch.Put(key, value)
		count++
		if count >= 1000 {
			err = backupDB.Write(batch, nil)
			if err != nil {
				return err
			}
			batch.Reset()
			count = 0
		}
	}
	if batch.Len() > 0 {
		err = backupDB.Write(batch, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) RestoreFrom(backupPath string) error {
	backupDB, err := leveldb.OpenFile(backupPath, nil)
	if err != nil {
		return err
	}
	defer backupDB.Close()

	iter := backupDB.NewIterator(nil, nil)
	defer iter.Release()

	batch := new(leveldb.Batch)
	count := 0
	for iter.Next() {
		key := append([]byte{}, iter.Key()...)
		value := append([]byte{}, iter.Value()...)
		batch.Put(key, value)
		count++
		if count >= 1000 {
			err = db.DB.Write(batch, nil)
			if err != nil {
				return err
			}
			batch.Reset()
			count = 0
		}
	}
	if batch.Len() > 0 {
		err = db.DB.Write(batch, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Close() error {
	return db.DB.Close()
}

// common functions
func makeHashKey(name, key []byte) []byte {
	return Bconcat(HashPrefix, name, SplitChar, key)
}

func makeHashKeyPrefix(name string) []byte {
	return Bconcat(HashPrefix, []byte(name), SplitChar)
}

func makeKeyScore(name string, key []byte) []byte {
	return Bconcat(ZsetScorePrefix, []byte(name), SplitChar, key)
}

func makeKeyScorePrefix(name string) []byte {
	return Bconcat(ZsetScorePrefix, []byte(name), SplitChar)
}

func makeScoreKey(name string, score, key []byte) []byte {
	return Bconcat(ZsetKeyPrefix, []byte(name), SplitChar, score, SplitChar, key)
}

func makeScoreKeyPrefix(name string) []byte {
	return Bconcat(ZsetKeyPrefix, []byte(name), SplitChar)
}

// hash functions
func (db *DB) Hset(name string, key, val []byte) error {
	realKey := makeHashKey([]byte(name), key)
	return db.Put(realKey, val, nil)
}

func (db *DB) Hget(name string, key []byte) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []RawData{},
	}
	realKey := makeHashKey([]byte(name), key)
	val, err := db.Get(realKey, nil)
	if err != nil {
		r.State = err.Error()
		return r
	}
	r.State = replyOK
	r.Data = append(r.Data, val)
	return r
}

func (db *DB) Hmset(name string, kvs ...[]byte) error {
	if len(kvs) == 0 || len(kvs)%2 != 0 {
		return errors.New("kvs len must is an even number")
	}
	batch := new(leveldb.Batch)
	for i := 0; i < (len(kvs) - 1); i += 2 {
		batch.Put(makeHashKey([]byte(name), kvs[i]), kvs[i+1])
	}
	return db.Write(batch, nil)
}

func (db *DB) Hmget(name string, keys [][]byte) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []RawData{},
	}
	for _, key := range keys {
		val, err := db.Get(makeHashKey([]byte(name), key), nil)
		if err != nil {
			continue
		}
		r.Data = append(r.Data, key, val)
	}
	if len(r.Data) > 0 {
		r.State = replyOK
	}
	return r
}

func (db *DB) Hincr(name string, key []byte, step int64) (newNum uint64, err error) {
	realKey := makeHashKey([]byte(name), key)
	var oldNum uint64
	var val []byte
	val, err = db.Get(realKey, nil)
	if err == nil {
		oldNum = BytesToUint64(val)
	}
	if step > 0 {
		if (scoreMax - uint64(step)) < oldNum {
			err = errors.New("overflow number")
			return
		}
		newNum = oldNum + uint64(step)
	} else {
		if uint64(-step) > oldNum {
			err = errors.New("overflow number")
			return
		}
		newNum = oldNum - uint64(-step)
	}

	err = db.Put(realKey, Uint64ToBytes(newNum), nil)
	if err != nil {
		newNum = 0
		return
	}
	return
}

func (db *DB) HgetInt(name string, key []byte) uint64 {
	realKey := makeHashKey([]byte(name), key)
	val, err := db.Get(realKey, nil)
	if err != nil {
		return 0
	}
	return BytesToUint64(val)
}

func (db *DB) HhasKey(name string, key []byte) bool {
	realKey := makeHashKey([]byte(name), key)
	has, err := db.Has(realKey, nil)
	if err != nil {
		return false
	}
	return has
}

func (db *DB) Hdel(name string, key []byte) error {
	realKey := makeHashKey([]byte(name), key)
	return db.Delete(realKey, nil)
}

func (db *DB) Hmdel(name string, keys [][]byte) error {
	batch := new(leveldb.Batch)
	for _, key := range keys {
		realKey := makeHashKey([]byte(name), key)
		batch.Delete(realKey)
	}
	return db.Write(batch, nil)
}

func (db *DB) HdelBucket(name string) error {
	batch := new(leveldb.Batch)
	keyPrefix := makeHashKeyPrefix(name)
	iter := db.NewIterator(util.BytesPrefix(keyPrefix), nil)
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return err
	}
	return db.Write(batch, nil)
}

func (db *DB) Hscan(name string, keyStart []byte, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []RawData{},
	}
	keyPrefix := makeHashKeyPrefix(name)
	realKey := Bconcat(keyPrefix, keyStart)
	keyPrefixLen := len(keyPrefix)
	n := 0
	sliceRange := util.BytesPrefix(keyPrefix)
	if len(realKey) > keyPrefixLen {
		sliceRange.Start = realKey
	} else {
		realKey = sliceRange.Start
	}
	iter := db.NewIterator(sliceRange, nil)
	for ok := iter.First(); ok; ok = iter.Next() {
		if bytes.Compare(realKey, iter.Key()) == -1 {
			r.Data = append(r.Data,
				append([]byte{}, iter.Key()[keyPrefixLen:]...),
				append([]byte{}, iter.Value()...),
			)
			n++
			if n == limit {
				break
			}
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		r.State = err.Error()
		r.Data = []RawData{}
		return r
	}
	if n > 0 {
		r.State = replyOK
	}
	return r
}

func (db *DB) Hrscan(name string, keyStart []byte, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []RawData{},
	}

	keyPrefix := makeHashKeyPrefix(name)
	realKey := Bconcat(keyPrefix, keyStart)
	keyPrefixLen := len(keyPrefix)
	n := 0

	sliceRange := util.BytesPrefix(keyPrefix)
	if len(realKey) > keyPrefixLen {
		sliceRange.Limit = realKey
	}

	iter := db.NewIterator(sliceRange, nil)
	for ok := iter.Last(); ok; ok = iter.Prev() {
		r.Data = append(r.Data,
			append([]byte{}, iter.Key()[keyPrefixLen:]...),
			append([]byte{}, iter.Value()...),
		)
		n++
		if n == limit {
			break
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		r.State = err.Error()
		r.Data = []RawData{}
		return r
	}

	if n > 0 {
		r.State = replyOK
	}

	return r
}

func (db *DB) Hprefix(name string, prefix []byte, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []RawData{},
	}
	realKey := makeHashKey([]byte(name), prefix) // keyPrefix
	keyPrefixLen := len(realKey)
	n := 0
	sliceRange := util.BytesPrefix(realKey)
	if len(realKey) > keyPrefixLen {
		sliceRange.Start = realKey
	} else {
		realKey = sliceRange.Start
	}
	iter := db.NewIterator(sliceRange, nil)
	for ok := iter.First(); ok; ok = iter.Next() {
		if bytes.Compare(realKey, iter.Key()) == -1 {
			r.Data = append(r.Data,
				append([]byte{}, iter.Key()[keyPrefixLen:]...),
				append([]byte{}, iter.Value()...),
			)
			n++
			if n == limit {
				break
			}
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		r.State = err.Error()
		r.Data = []RawData{}
		return r
	}
	if n > 0 {
		r.State = replyOK
	}
	return r
}

// zset functions
func (db *DB) Zset(name string, key []byte, val uint64) error {
	score := Uint64ToBytes(val)
	keyScore := makeKeyScore(name, key)           // key / score
	newScoreKey := makeScoreKey(name, score, key) // name+score+key / nil

	oldScore, _ := db.Get(keyScore, nil)
	if !bytes.Equal(oldScore, score) {
		batch := new(leveldb.Batch)
		batch.Put(keyScore, score)
		batch.Put(newScoreKey, nil)
		batch.Delete(makeScoreKey(name, oldScore, key))
		return db.Write(batch, nil)
	}
	return nil
}

func (db *DB) Zget(name string, key []byte) uint64 {
	val, err := db.Get(makeKeyScore(name, key), nil)
	if err != nil {
		return 0
	}
	return BytesToUint64(val)
}

func (db *DB) Zincr(name string, key []byte, step int64) (uint64, error) {
	keyScore := makeKeyScore(name, key) // key / score
	score := db.Zget(name, key)         // get old score
	oldScoreB := Uint64ToBytes(score)   // old score byte
	if step > 0 {
		if (scoreMax - uint64(step)) < score {
			return 0, errors.New("overflow number")
		}
		score += uint64(step)
	} else {
		if uint64(-step) > score {
			return 0, errors.New("overflow number")
		}
		score -= uint64(-step)
	}

	newScoreB := Uint64ToBytes(score)

	batch := new(leveldb.Batch)
	batch.Put(keyScore, newScoreB)
	batch.Put(makeScoreKey(name, newScoreB, key), nil)
	batch.Delete(makeScoreKey(name, oldScoreB, key))
	err := db.Write(batch, nil)
	if err != nil {
		return 0, err
	}
	return score, nil
}

func (db *DB) ZhasKey(name string, key []byte) bool {
	keyScore := makeKeyScore(name, key)
	has, err := db.Has(keyScore, nil)
	if err != nil {
		return false
	}
	return has
}

func (db *DB) Zdel(name string, key []byte) error {
	keyScore := makeKeyScore(name, key) // key / score
	oldScore, err := db.Get(keyScore, nil)
	if err != nil {
		return err
	}
	batch := new(leveldb.Batch)
	batch.Delete(keyScore)
	batch.Delete(makeScoreKey(name, oldScore, key))
	return db.Write(batch, nil)
}

func (db *DB) ZdelBucket(name string) error {
	batch := new(leveldb.Batch)
	iter := db.NewIterator(util.BytesPrefix(makeKeyScorePrefix(name)), nil)
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return err
	}

	iter = db.NewIterator(util.BytesPrefix(makeScoreKeyPrefix(name)), nil)
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		return err
	}

	return db.Write(batch, nil)
}

func (db *DB) Zmset(name string, kvs [][]byte) error {
	if len(kvs) == 0 || len(kvs)%2 != 0 {
		return errors.New("kvs len must is an even number")
	}
	batch := new(leveldb.Batch)
	for i := 0; i < (len(kvs) - 1); i += 2 {
		key, score := kvs[i], kvs[i+1]

		keyScore := makeKeyScore(name, key)           // key / score
		newScoreKey := makeScoreKey(name, score, key) // name+score+key / nil

		oldScore, _ := db.Get(keyScore, nil)
		if !bytes.Equal(oldScore, score) {
			batch.Put(keyScore, score)
			batch.Put(newScoreKey, nil)
			batch.Delete(makeScoreKey(name, oldScore, key))
		}
	}
	return db.Write(batch, nil)
}

func (db *DB) Zmget(name string, keys [][]byte) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []RawData{},
	}

	for _, key := range keys {
		val, err := db.Get(makeKeyScore(name, key), nil)
		if err != nil {
			continue
		}
		r.Data = append(r.Data, key, val)
	}
	if len(r.Data) > 0 {
		r.State = replyOK
	}
	return r
}

func (db *DB) Zmdel(name string, keys [][]byte) error {
	batch := new(leveldb.Batch)
	for _, key := range keys {
		keyScore := makeKeyScore(name, key) // key / score
		oldScore, err := db.Get(keyScore, nil)
		if err != nil {
			continue
		}
		batch.Delete(keyScore)
		batch.Delete(makeScoreKey(name, oldScore, key))
	}
	return db.Write(batch, nil)
}

func (db *DB) Zscan(name string, keyStart, scoreStart []byte, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []RawData{},
	}

	if len(scoreStart) == 0 {
		scoreStart = Uint64ToBytes(scoreMin)
	}

	keyPrefix := makeScoreKeyPrefix(name)
	realKey := makeScoreKey(name, scoreStart, keyStart)
	scoreBeginIndex := len(keyPrefix)
	scoreEndIndex := scoreBeginIndex + scoreByteLen
	keyBeginIndex := scoreBeginIndex + scoreByteLen + 1
	n := 0
	sliceRange := util.BytesPrefix(keyPrefix)
	if len(keyStart) == 0 {
		realKey = util.BytesPrefix(Bconcat(keyPrefix, scoreStart, SplitChar)).Limit
	}
	sliceRange.Start = realKey
	iter := db.NewIterator(sliceRange, nil)
	for ok := iter.First(); ok; ok = iter.Next() {
		if bytes.Compare(realKey, iter.Key()) == -1 {
			r.Data = append(r.Data,
				append([]byte{}, iter.Key()[keyBeginIndex:]...),                // key
				append([]byte{}, iter.Key()[scoreBeginIndex:scoreEndIndex]...), // score
			)
			n++
			if n == limit {
				break
			}
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		r.State = err.Error()
		r.Data = []RawData{}
		return r
	}
	if n > 0 {
		r.State = replyOK
	}
	return r
}

func (db *DB) Zrscan(name string, keyStart, scoreStart []byte, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []RawData{},
	}

	if len(scoreStart) == 0 {
		scoreStart = Uint64ToBytes(scoreMax)
	}

	keyPrefix := makeScoreKeyPrefix(name)
	realKey := makeScoreKey(name, scoreStart, keyStart)
	scoreBeginIndex := len(keyPrefix)
	scoreEndIndex := scoreBeginIndex + scoreByteLen
	keyBeginIndex := scoreBeginIndex + scoreByteLen + 1
	n := 0
	sliceRange := util.BytesPrefix(keyPrefix)
	if len(keyStart) == 0 {
		realKey = util.BytesPrefix(Bconcat(keyPrefix, scoreStart, SplitChar)).Start
	}
	sliceRange.Limit = realKey
	iter := db.NewIterator(sliceRange, nil)
	for ok := iter.Last(); ok; ok = iter.Prev() {
		if bytes.Compare(realKey, iter.Key()) == 1 {
			r.Data = append(r.Data,
				append([]byte{}, iter.Key()[keyBeginIndex:]...),                // key
				append([]byte{}, iter.Key()[scoreBeginIndex:scoreEndIndex]...), // score
			)
			n++
			if n == limit {
				break
			}
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		r.State = err.Error()
		r.Data = []RawData{}
		return r
	}
	if n > 0 {
		r.State = replyOK
	}
	return r
}

// helper functions
func Bconcat(parts ...[]byte) []byte {
	var total int
	for _, p := range parts {
		total += len(p)
	}
	b := make([]byte, 0, total)
	for _, p := range parts {
		b = append(b, p...)
	}
	return b
}

func BytesToUint64(v []byte) uint64 {
	if len(v) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v[:8])
}

func Uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// DigitStringToBytes returns an 8-byte big endian representation of a digit string.
// Example: "123456" -> uint64(123456) -> []byte{...}
func DigitStringToBytes(s string) []byte {
	i, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return []byte("")
	}
	return Uint64ToBytes(i)
}

// BytesToDigitString converts an 8-byte big endian byte slice to a digit string.
// Example: []byte{...} -> uint64(123456) -> "123456"
func BytesToDigitString(b []byte) string {
	return strconv.FormatUint(binary.BigEndian.Uint64(b), 10)
}

// DigitStringToUint64 parses a digit string into a uint64 number.
// Example: "123456" -> 123456
func DigitStringToUint64(s string) uint64 {
	i, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return i
}

func (r *Reply) OK() bool {
	return r.State == replyOK
}

func (r *Reply) NotFound() bool {
	return r.State == replyNotFound
}

func (r *Reply) Bytes() []byte {
	return r.bytex()
}

func (r *Reply) bytex() RawData {
	if len(r.Data) > 0 {
		return r.Data[0]
	}
	return nil
}

// String is a convenience wrapper over Get for string value.
func (r *Reply) String() string {
	return r.bytex().String()
}

// Int is a convenience wrapper over Get for int value of a hashmap.
func (r *Reply) Int() int {
	return r.bytex().Int()
}

// Int64 is a convenience wrapper over Get for int64 value of a hashmap.
func (r *Reply) Int64() int64 {
	return r.bytex().Int64()
}

// Uint is a convenience wrapper over Get for uint value of a hashmap.
func (r *Reply) Uint() uint {
	return r.bytex().Uint()
}

// Uint64 is a convenience wrapper over Get for uint64 value of a hashmap.
func (r *Reply) Uint64() uint64 {
	return r.bytex().Uint64()
}

func (r *Reply) Json() gjson.Result {
	return r.bytex().Json()
}

// List retrieves the key/value pairs from reply of a hashmap.
func (r *Reply) List() []Entry {
	if len(r.Data) < 1 {
		return []Entry{}
	}
	list := make([]Entry, len(r.Data)/2)
	j := 0
	for i := 0; i < (len(r.Data) - 1); i += 2 {
		list[j] = Entry{r.Data[i], r.Data[i+1]}
		j++
	}
	return list
}

// Dict retrieves the key/value pairs from reply of a hashmap.
func (r *Reply) Dict() map[string][]byte {
	if len(r.Data) < 1 {
		return map[string][]byte{}
	}
	dict := make(map[string][]byte, len(r.Data)/2)
	for i := 0; i < (len(r.Data) - 1); i += 2 {
		dict[string(r.Data[i])] = r.Data[i+1]
	}
	return dict
}

func (r *Reply) KvLen() int {
	return len(r.Data) / 2
}

func (r *Reply) KvEach(fn func(key, value RawData)) int {
	for i := 0; i < (len(r.Data) - 1); i += 2 {
		fn(r.Data[i], r.Data[i+1])
	}
	return r.KvLen()
}

func (b RawData) Bytes() []byte {
	return b
}

func (b RawData) String() string {
	return string(b)
}

// Int is a convenience wrapper over Get for int value of a hashmap.
func (b RawData) Int() int {
	return int(b.Uint64())
}

// Int64 is a convenience wrapper over Get for int64 value of a hashmap.
func (b RawData) Int64() int64 {
	return int64(b.Uint64())
}

// Uint is a convenience wrapper over Get for uint value of a hashmap.
func (b RawData) Uint() uint {
	return uint(b.Uint64())
}

// Uint64 is a convenience wrapper over Get for uint64 value of a hashmap.
func (b RawData) Uint64() uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func (b RawData) Json() gjson.Result {
	return gjson.ParseBytes(b)
}
