// 2021.12.12
// 配置

package config

import (
	"bufio"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"

	"ljr-redis/lib/logger"
)

// 全局配置属性
type ServerProperties struct {
	Bind           string `cfg:"bind"`
	Port           int    `cfg:"port"`
	AppendOnly     bool   `cfg:"appendOnly"`
	AppendFileName string `cfg:"appendFileName"`
	MaxClients     int    `cfg:"maxclients"`
	RequirePass    string `cfg:"requirepass"`
	Databases      int    `cfg:"databases"`

	Peers []string `cfg:"peers"`
	Self  string   `cfg:"self"`
}

// 全局配置
var Properties *ServerProperties

// main 函数前执行
func init() {
	// 默认配置
	Properties = &ServerProperties{
		Bind:       "127.0.0.1",
		Port:       6379,
		AppendOnly: false,
	}
}

// 解析配置文件
func parse(src io.Reader) *ServerProperties {
	config := &ServerProperties{}

	// 读取配置文件
	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(src)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && line[0] == '#' {
			// 空行 注释
			continue
		}

		// 空格分割
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 {
			key := line[0:pivot]
			value := strings.Trim(line[pivot+1:], " ")
			rawMap[strings.ToLower(key)] = value
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}

	// 配置解析
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldValue := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok {
			key = field.Name
		}

		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			switch field.Type.Kind() {
			case reflect.String:
				fieldValue.SetString(value)

			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldValue.SetInt(intValue)
				}

			case reflect.Bool:
				// boolValue := ("yes" == value)
				boolValue := (value == "yes")
				fieldValue.SetBool(boolValue)

			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")
					fieldValue.Set(reflect.ValueOf(slice))
				}
			}

		}
	}

	return config
}

// 读取配置文件
func SetupConfig(configFileName string) {
	file, err := os.Open(configFileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// 解析配置文件
	Properties = parse(file)
}
