# bitcask-kv

[paper](https://riak.com/assets/bitcask-intro.pdf)

日志型k-v存储引擎

rosedb 的 mini 版本，帮助理解 bitcask 存储模型以及 rosedb 项目。

需要说明的是，minidb 没有实现 bitcask 模型的多个数据文件的机制，为了简单，我只使用了一个数据文件进行读写。但这并不妨碍你理解 bitcask 模型。

[introduction](https://mp.weixin.qq.com/s/s8s6VtqwdyjthR6EtuhnUA)