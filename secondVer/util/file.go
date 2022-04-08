package util

import (
	"io"
	"os"
	"path/filepath"
)

// 基础工具方法，文件相关

func PathExist(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

// copydir from src to dst
func CopyDir(src, dst string) error {
	var (
		err     error
		dir     []os.DirEntry
		srcInfo os.FileInfo
	)

	if srcInfo, err = os.Stat(src); err != nil {
		return err
	}
	if err = os.MkdirAll(dst, srcInfo.Mode()); err != nil {
		return err
	}
	// 获取源地址下的所有文件
	if dir, err = os.ReadDir(src); err != nil {
		return err
	}

	for _, fd := range dir {
		srcPath := filepath.Join(src, fd.Name())
		dstPath := filepath.Join(dst, fd.Name())

		if fd.IsDir() { // fd 是路径
			if err = CopyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err = CopyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}

	return nil
}

// 拷贝文件
func CopyFile(src, dst string) error {
	var (
		err     error
		srcFile *os.File
		dstFile *os.File
		srcInfo os.FileInfo
	)

	if srcFile, err = os.Open(src); err != nil {
		return err
	}
	defer srcFile.Close()

	if dstFile, err = os.Create(dst); err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err = io.Copy(dstFile, srcFile); err != nil {
		return err
	}
	// 获取src 的文件基本信息，方便后续改变dstFile 的mode
	if srcInfo, err = os.Stat(src); err != nil {
		return err
	}

	return os.Chmod(dst, srcInfo.Mode())
}
