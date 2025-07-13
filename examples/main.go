package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/naipad/loach"
)

const demodb = "testdb"

func main() {
	os.RemoveAll(demodb)
	db, err := loach.OpenDefault(demodb)
	if err != nil {
		log.Fatalln(err)
	}
	vals := [][]byte{
		[]byte("my value"),
		[]byte("byte value"),
	}

	name := "mytest"
	key := []byte("mykey")

	fmt.Println("-------------- test Hset and Hget -------------- ")
	for _, v := range vals {
		err = db.Hset(name, key, v)
		if err != nil {
			log.Fatalln(err)
		}
		rs := db.Hget(name, key)
		if !rs.OK() {
			log.Fatalln(rs.State)
		}
		fmt.Println(v, rs.String(), rs.Bytes())
	}

	fmt.Println("-------------- test NotFound -------------- ")
	// notfound
	rs := db.Hget("mytest2", []byte("mykey2"))
	if !rs.NotFound() {
		log.Fatalln(rs.State)
	}
	fmt.Println(rs.State, string(rs.String()))

	fmt.Println("-------------- test Hmset and Hmget -------------- ")
	var kvs [][]byte
	for i := 1; i < 9; i++ {
		kvs = append(kvs, []byte("k"+strconv.Itoa(i)))
	}

	err = db.Hmset(name, kvs...)
	if err != nil {
		log.Fatalln("Hmset error:", err)
	}
	rs = db.Hmget(name, [][]byte{[]byte("k1"), []byte("k2")})
	if !rs.OK() {
		log.Fatalln("Hmget not ok")
	}
	rs.KvEach(func(key, value loach.RawData) {
		fmt.Printf("key:%s,val:%v\n", key.String(), value.String())
	})

	fmt.Println("-------------- test Hdel Hincr and HgetInt -------------- ")
	fmt.Println(db.Hdel("count", []byte("mytest")))
	fmt.Println(db.Hincr("count", []byte("mytest"), 12))
	fmt.Println(db.HgetInt("count", []byte("mytest"))) // 12

	fmt.Println("-------------- test Hmdel -------------- ")

	fmt.Println(db.Hmdel(name, [][]byte{[]byte("k1"), []byte("k2")}))
	fmt.Println(db.Hget(name, []byte("k1")))
	fmt.Println(db.Hset(name, []byte("k1"), []byte("v11")))
	fmt.Println(db.Hget(name, []byte("k1")))
	fmt.Println(db.HdelBucket(name))
	fmt.Println(db.Hget(name, []byte("k1")))
	fmt.Println(db.HgetInt("count", []byte("mytest"))) // 12

	fmt.Println(db.HdelBucket(name))
	_ = db.Hmset(name, kvs...)
	fmt.Println("-------------- test Hmget -------------- ")
	db.Hmget(name, [][]byte{[]byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")}).KvEach(func(key, value loach.RawData) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("-------------- test Hscan -------------- ")
	db.Hscan(name, []byte("k2"), 2).KvEach(func(key, value loach.RawData) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("-------------- test Hscan 2 -------------- ")
	db.Hscan(name, []byte("k6"), 4).KvEach(func(key, value loach.RawData) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("-------------- test Hscan 3 -------------- ")
	db.Hscan(name, nil, 9).KvEach(func(key, value loach.RawData) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("-------------- test Hrscan-------------- ")
	db.Hrscan(name, []byte("k5"), 2).KvEach(func(key, value loach.RawData) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("-------------- test Hrscan 2 -------------- ")
	db.Hrscan(name, []byte("k7"), 4).KvEach(func(key, value loach.RawData) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("-------------- test Hrscan 3 -------------- ")
	db.Hrscan(name, nil, 9).KvEach(func(key, value loach.RawData) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("-------------- test Hscan page -------------- ")
	var keyStart []byte
	ii := 0
	for {
		rs := db.Hscan(name, keyStart, 2)
		if !rs.OK() {
			break
		}
		rs.KvEach(func(key, value loach.RawData) {
			fmt.Println(key, value)
			keyStart = key
			ii++
		})
	}
	fmt.Println(ii)

	fmt.Println("-------------- test Zset and Zget -------------- ")
	k := []byte("k1")
	fmt.Println(db.Zset(name, k, 12))
	fmt.Println(db.Zget(name, k))

	fmt.Println(db.Zincr(name, k, 2))
	fmt.Println(db.Zincr(name, k, -3))
	fmt.Println(db.Zdel(name, k))
	fmt.Println(db.Zget(name, k))
	fmt.Println(db.Zincr(name, k, 2))
	fmt.Println(db.Zincr(name, []byte("k2"), 2))
	fmt.Println(db.ZdelBucket(name))
	fmt.Println(db.Zincr(name, k, 2))
	fmt.Println(db.Zincr(name, []byte("k2"), 2))

	fmt.Println("-------------- test Zmset -------------- ")
	fmt.Println(db.Zmset(name, [][]byte{
		[]byte("k1"), loach.Uint64ToBytes(1),
		[]byte("k2"), loach.Uint64ToBytes(2),
		[]byte("k3"), loach.Uint64ToBytes(3),
		[]byte("k4"), loach.Uint64ToBytes(4),
		[]byte("k5"), loach.Uint64ToBytes(5),
		[]byte("k6"), loach.Uint64ToBytes(2),
		[]byte("k7"), loach.Uint64ToBytes(5),
		[]byte("k8"), loach.Uint64ToBytes(5),
		[]byte("k9"), loach.Uint64ToBytes(5),
		[]byte("k10"), loach.Uint64ToBytes(5),
		[]byte("k11"), loach.Uint64ToBytes(1),
		[]byte("k12"), loach.Uint64ToBytes(2),
		[]byte("k13"), loach.Uint64ToBytes(3),
		[]byte("k14"), loach.Uint64ToBytes(4),
		[]byte("k15"), loach.Uint64ToBytes(5),
		[]byte("k16"), loach.Uint64ToBytes(2),
		[]byte("k17"), loach.Uint64ToBytes(5),
		[]byte("k18"), loach.Uint64ToBytes(5),
		[]byte("k19"), loach.Uint64ToBytes(5),
		[]byte("k20"), loach.Uint64ToBytes(5),
	}))

	fmt.Println("-------------- test Zmget -------------- ")
	db.Zmget(name, [][]byte{[]byte("k2"), []byte("k3"), []byte("k4")}).KvEach(func(key, value loach.RawData) {
		fmt.Println(key.String(), loach.BytesToUint64(value))
	})

	fmt.Println("-------------- test Zscan -------------- ")
	db.Zscan(name, []byte("k2"), loach.Uint64ToBytes(2), 30).KvEach(func(key, value loach.RawData) {
		fmt.Println(key.String(), loach.BytesToUint64(value))
	})

	fmt.Println("-------------- test Zscan page -------------- ")
	keyStart = keyStart[:0]
	var scourStart []byte
	ii = 0
	for {
		rs := db.Zscan(name, keyStart, scourStart, 2)
		if !rs.OK() {
			break
		}
		rs.KvEach(func(key, value loach.RawData) {
			fmt.Println(key.String(), loach.BytesToUint64(value))
			keyStart = key
			scourStart = value
			ii++
		})
	}
	fmt.Println(ii)

	fmt.Println("-------------- test Zrscan -------------- ")
	db.Zrscan(name, nil, nil, 30).KvEach(func(key, value loach.RawData) {
		fmt.Println(key.String(), loach.BytesToUint64(value))
	})

	fmt.Println("-------------- test Zrscan page -------------- ")
	keyStart, scourStart = nil, nil
	ii = 0
	for {
		rs := db.Zrscan(name, keyStart, scourStart, 2)
		if !rs.OK() {
			break
		}
		rs.KvEach(func(key, value loach.RawData) {
			fmt.Println(key.String(), loach.BytesToUint64(value))
			keyStart = key
			scourStart = value
			ii++
		})
	}
	fmt.Println(ii)

	fmt.Println("-------------- test Hprefix -------------- ")
	name = "n1"
	db.Hset(name, loach.Bconcat(loach.Uint64ToBytes(1), loach.Uint64ToBytes(1)), nil)
	db.Hset(name, loach.Bconcat(loach.Uint64ToBytes(1), loach.Uint64ToBytes(2)), nil)
	db.Hset(name, loach.Bconcat(loach.Uint64ToBytes(1), loach.Uint64ToBytes(3)), nil)
	db.Hset(name, loach.Bconcat(loach.Uint64ToBytes(1), loach.Uint64ToBytes(4)), nil)
	db.Hset(name, loach.Bconcat(loach.Uint64ToBytes(1), loach.Uint64ToBytes(5)), nil)
	db.Hset(name, loach.Bconcat(loach.Uint64ToBytes(1), loach.Uint64ToBytes(6)), nil)

	db.Hset(name, loach.Bconcat(loach.Uint64ToBytes(2), loach.Uint64ToBytes(1)), nil)
	db.Hset(name, loach.Bconcat(loach.Uint64ToBytes(2), loach.Uint64ToBytes(2)), nil)
	db.Hset(name, loach.Bconcat(loach.Uint64ToBytes(2), loach.Uint64ToBytes(3)), nil)
	db.Hset(name, loach.Bconcat(loach.Uint64ToBytes(2), loach.Uint64ToBytes(4)), nil)
	db.Hset(name, loach.Bconcat(loach.Uint64ToBytes(2), loach.Uint64ToBytes(5)), nil)
	db.Hset(name, loach.Bconcat(loach.Uint64ToBytes(2), loach.Uint64ToBytes(6)), nil)

	db.Hset("n2", loach.Bconcat(loach.Uint64ToBytes(1), []byte("a")), []byte("av"))
	db.Hset("n2", loach.Bconcat(loach.Uint64ToBytes(1), []byte("b")), []byte("bv"))
	db.Hset("n2", loach.Bconcat(loach.Uint64ToBytes(1), []byte("c")), []byte("cv"))
	db.Hset("n2", loach.Bconcat(loach.Uint64ToBytes(1), []byte("d")), []byte("dv"))

	db.Hset("n2", loach.Bconcat([]byte("qq"), []byte("aq")), []byte("avq"))
	db.Hset("n2", loach.Bconcat([]byte("qq"), []byte("bq")), []byte("bvq"))
	db.Hset("n2", loach.Bconcat([]byte("qq"), []byte("qc")), []byte("cvq"))
	db.Hset("n2", loach.Bconcat([]byte("qq"), []byte("dd")), []byte("dvq"))

	db.Hprefix(name, loach.Uint64ToBytes(1), 8).KvEach(func(key, value loach.RawData) {
		fmt.Println(loach.BytesToUint64(key[:8]))
	})

	fmt.Println("-------------- test Hprefix 2,8 -------------- ")
	db.Hprefix(name, loach.Uint64ToBytes(2), 8).KvEach(func(key, value loach.RawData) {
		fmt.Println(loach.BytesToUint64(key[:8]))
	})

	fmt.Println("-------------- test Hprefix 0,8 -------------- ")
	db.Hprefix(name, loach.Uint64ToBytes(0), 8).KvEach(func(key, value loach.RawData) {
		fmt.Println(loach.BytesToUint64(key[:8]), loach.BytesToUint64(key[8:]))
	})

	fmt.Println("-------------- test Hprefix 0,8(string) -------------- ")
	db.Hprefix("n2", loach.Uint64ToBytes(1), 8).KvEach(func(key, value loach.RawData) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("-------------- test Hprefix ([]byte),8 -------------- ")
	db.Hprefix("n2", []byte("qq"), 8).KvEach(func(key, value loach.RawData) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("-------------- test Hscan 1,8 -------------- ")
	db.Hscan(name, loach.Uint64ToBytes(1), 8).KvEach(func(key, value loach.RawData) {
		fmt.Println(loach.BytesToUint64(key[:8]), loach.BytesToUint64(key[8:]))
	})

	fmt.Println("-------------- test Hset and Hscan (big num) -------------- ")
	num := uint64(1583660405608876000)
	db.Hset("aaa", loach.Uint64ToBytes(num), nil)
	db.Hset("aaa", loach.Uint64ToBytes(num+1), nil)
	db.Hset("aaa", loach.Uint64ToBytes(num+2), nil)

	db.Hscan("aaa", loach.Uint64ToBytes(num), 4).KvEach(func(key, value loach.RawData) {
		fmt.Println(loach.BytesToUint64(key.Bytes()))
	})

	fmt.Println("-------------- test Json data -------------- ")
	db.Hset("user:tom", []byte("tom"), []byte(`{"name":"Alice","age":30}`))
	r := db.Hget("user:tom", []byte("tom"))
	user := r.Json()
	fmt.Printf("user:%v,age:%v\n", user.Get("name").String(), user.Get("age").String())

	if err := db.Close(); err == nil {
		os.RemoveAll(demodb)
	} else {
		fmt.Println(err.Error())
	}
}
