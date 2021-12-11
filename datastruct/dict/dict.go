// 2021.12.11
// 字典数据类型

package dict

// 用于遍历 dict 如果返回 false 则 break 遍历
type Consumer func(key string, val interface{}) bool

// 抽象 dict 类型
type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int
	Put(key string, val interface{}) (result int)
}
