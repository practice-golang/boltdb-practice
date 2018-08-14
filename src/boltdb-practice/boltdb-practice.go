package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"time"

	bolt "github.com/coreos/bbolt"
)

// Book : Book 정보
type Book struct {
	ID     uint
	Title  string
	Author string
}

func (b *Book) gobEncode() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(b)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gobDecode(data []byte) (*Book, error) {
	var b *Book
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func setData(key []byte, value []byte, bucket string, db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		return bkt.Put(key, value)
	})
}

func getLastKey(bucket string, db *bolt.DB) (uint, error) {
	var id uint64
	err := db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			panic(err)
		}
		err = nil

		c := bkt.Cursor()
		k, _ := c.Last()
		id, err = strconv.ParseUint(string(k), 16, 64)

		return err
	})

	return uint(id), err
}

func getData(id string, bucket string, db *bolt.DB) ([]Book, error) {
	books := make([]Book, 0)

	err := db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			return nil
		}

		c := bkt.Cursor()

		idNum, _ := strconv.Atoi(id)
		if idNum > 0 {
			c.Seek([]byte(id))
		} else {
			for k, v := c.First(); k != nil; k, v = c.Next() {
				data, _ := gobDecode(v)
				books = append(books, *data)

				// fmt.Println(string(v))
				// fmt.Printf("key, value: %s , %s\n", k, *data)
				// fmt.Printf("key, value: %s , %s, %s\n", k, data.Title, data.Author)
			}
		}

		return nil
	})

	return books, err
}

func getBuckets(db *bolt.DB) ([]string, error) {
	var names = make([]string, 0)
	err := db.View(func(tx *bolt.Tx) error {
		tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			names = append(names, string(name))
			return nil
		})
		return nil
	})

	return names, err
}

func main() {
	bucket := "books"

	db, err := bolt.Open("books.db", 0644, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	bkts, _ := getBuckets(db)
	fmt.Printf("%+v\n", bkts)

	lastID, _ := getLastKey(bucket, db)

	var books []Book

	lastID++
	book := Book{lastID, "My First Book", "Human"}
	books = append(books, book)
	lastID++
	book = Book{ID: lastID, Title: "My Second Book", Author: "Animal"}
	books = append(books, book)

	for _, v := range books {
		data, _ := v.gobEncode()

		// Book.ID를 인덱스로 넣는 경우
		setData([]byte(strconv.FormatInt(int64(v.ID), 10)), data, bucket, db)
	}

	recevedBooks, _ := getData("", bucket, db)

	fmt.Printf("%+v\n", recevedBooks)
}
