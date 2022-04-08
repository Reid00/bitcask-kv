package logger

import (
	"testing"
)

func Test0x(t *testing.T) {
	err := "NOT FOUND"
	// 调用该方法，使用全局的_log 实例, // 红色
	Errorf("this is an error: %v", err)
}
