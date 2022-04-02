package minidb

import (
	"io"
	"os"
	"sync"
)

type MiniDB struct {
	indexes map[string]int64 // 内存中的索引结构
	dbFile  *DBFile          // 硬盘的数据文件
	dirPath string           // 数据目录
	mu      sync.RWMutex
}

// 开启一个数据库实例
func Open(dirPath string) (*MiniDB, error) {
	// 如果数据库目录不存在，则新建一个
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 加载数据文件
	dbFile, err := NewDBFile(dirPath)
	if err != nil {
		return nil, err
	}

	db := &MiniDB{
		dbFile:  dbFile,
		indexes: make(map[string]int64),
		dirPath: dirPath,
	}

	// 加载索引
	db.loadIndexesFromFile()

	return db, nil

}

// 合并数据
func (db *MiniDB) Merge() error {
	// no data, ignore
	if db.dbFile.Offset == 0 {
		return nil
	}

	var (
		validEntries []*Entry
		offset       int64
	)

	// 读取原数据文件中的Entry
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 内存中的索引状态是最新的， 直接对比过滤出有效的Entry
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()

	}

	if len(validEntries) > 0 {
		// 新建临时文件
		mergeDBFile, err := NewMergeDBFile(db.dirPath)
		if err != nil {
			return err
		}

		defer os.Remove(mergeDBFile.File.Name())

		// 重新写入有效的 entry
		for _, entry := range validEntries {
			writeOff := mergeDBFile.Offset
			if err := mergeDBFile.Write(entry); err != nil {
				return err
			}

			// 更新索引
			db.indexes[string(entry.Key)] = writeOff

		}

		// 获取文件名
		dbFileName := db.dbFile.File.Name()
		// 关闭j旧的文件
		db.dbFile.File.Close()
		// 删除旧的数据文件
		os.Remove(dbFileName)

		// 获取文件名
		mergerDBFileName := mergeDBFile.File.Name()
		// 关闭文件
		mergeDBFile.File.Close()
		// 临时数据文件 变更为新的数据文件
		os.Rename(mergerDBFileName, db.dirPath+string(os.PathSeparator)+FileName)

		db.dbFile = mergeDBFile
	}
	return nil

}

// 写入数据
func (db *MiniDB) Put(key, value []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock() //读写锁的互斥锁
	defer db.mu.Unlock()

	offset := db.dbFile.Offset // 现在db 中dbFile的entry 的长度，也是新入数据的偏移量
	// 封装成entry
	entry := NewEntry(key, value, PUT)

	// 把entry 写入db 的dbFile 中
	db.dbFile.Write(entry)

	// 写入在内存中的索引
	db.indexes[string(key)] = offset // 如果已经存在，会替换原来的值
	return
}

// 读取数据
func (db *MiniDB) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}

	db.mu.RLock() //读写锁的读锁
	defer db.mu.RUnlock()

	// 从内存中读取索引信息
	offset, ok := db.indexes[string(key)]
	// 如果不存在key
	if !ok {
		return
	}

	// 从磁盘中读取数据
	var e *Entry
	e, err = db.dbFile.Read(offset)
	if err != nil && err != io.EOF { // err != nil 已经可以判断， 如果 != nil 并且== EOF 代表空值
		return
	}

	if e != nil { // entry 是有效的
		val = e.Value
	}
	return
}

// 删除数据
func (db *MiniDB) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 从内存中加载索引信息, 判断是否存在
	_, ok := db.indexes[string(key)]
	// 如果不存在，忽略
	if !ok {
		return
	}

	// 封装成Entry 并写入, append 方式，即便是删除，也是用写入mark 为DEL 的entry
	e := NewEntry(key, nil, DEL)

	// 写入硬盘
	err = db.dbFile.Write(e)
	if err != nil {
		return
	}
	// 删除内存索引
	delete(db.indexes, string(key))
	return
}

// 从文件中加载索引信息
func (db *MiniDB) loadIndexesFromFile() {
	if db.dbFile == nil {
		return
	}

	var offset int64

	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			// 读取完毕
			if err == io.EOF {
				break
			}
			return
		}

		// 设置索引状态
		db.indexes[string(e.Key)] = offset
		if e.Mark == DEL {
			// 删除内存中的key
			delete(db.indexes, string(e.Key))
		}
		offset += e.GetSize()
	}

}
