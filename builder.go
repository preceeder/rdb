package rdb

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// RedisCmdDef 代表一个 Redis 命令的配置结构体
type RdSubCmd struct {
	CmdName        string //真正的 命令名, 当这个存在的时候就不会使用上层map的key作为命令名; 作用是检出同一个key对于同一个命令的不同参数的应对
	Params         string // 这里的数据 最后都会转化为 字符串数组， 数字也会变成字符串的， 一定要注意下
	Exp            func() time.Duration
	DefaultParams  map[string]any // 设置默认的参数
	NoUseKey       bool           // 不使用外层的key
	ReturnNilError bool           // 是否返回 redis的nil错误， 这个可以用来判断字段是不是在redis中， 批量操作的指令是不会有redis.nil错误的
}

// RedisCmdBuilder 用于构建 Redis 命令的结构体
type RdCmd struct {
	Key string
	CMD map[Command]RdSubCmd
}

// Build 构造 Redis 命令参数
func Build(ctx context.Context, cmd RdCmd, cmdName Command, args map[string]any, includeArgs ...any) ([]any, string, RdSubCmd) {
	if args == nil {
		args = map[string]any{}
	}
	subCmd, ok := cmd.CMD[cmdName]
	if !ok {
		panic(fmt.Errorf("unknown command: %s", cmdName))
	}
	// 填充默认参数
	for k, v := range subCmd.DefaultParams {
		if _, ok := args[k]; !ok {
			args[k] = v
		}
	}

	paramsStr := []any{}
	if subCmd.Params != "" {
		tempData := strings.Split(replaceMultiSpaceWithSingle(subCmd.Params), " ")
		for _, v := range tempData {
			paramsStr = append(paramsStr, string(highPerfReplace([]byte(v), args)))
		}
	}

	// 构造 key
	keyStr := cmd.Key
	if !subCmd.NoUseKey {
		keyStr = string(highPerfReplace([]byte(cmd.Key), args))
	}

	// 构造参数
	cmdArgs := []any{string(cmdName)}
	if keyStr != "" {
		cmdArgs = append(cmdArgs, keyStr)
	}
	cmdArgs = append(cmdArgs, paramsStr...)
	if len(includeArgs) > 0 {
		cmdArgs = append(cmdArgs, includeArgs...)
	}
	return cmdArgs, keyStr, subCmd
}

func replaceMultiSpaceWithSingle(s string) string {
	// 预编译正则表达式：匹配一个或多个空白字符（空格）
	spaceRegex := regexp.MustCompile(`\s+`)
	// 替换匹配到的连续空格为单个空格
	return spaceRegex.ReplaceAllString(strings.TrimSpace(s), " ")
}

func highPerfReplace(template []byte, replacements map[string]any) []byte {
	var result []byte
	buf := make([]byte, 0, 16)

	i := 0
	for i < len(template) {
		// 查找 '{{' 和 '}}' 分隔的占位符
		if i+1 < len(template) && template[i] == '{' && template[i+1] == '{' {
			end := bytes.Index(template[i:], []byte("}}"))
			if end == -1 {
				result = append(result, template[i:]...)
				break
			}
			key := string(template[i+2 : i+end])
			if val, found := replacements[key]; found {
				// 根据类型进行处理
				switch v := val.(type) {
				case string:
					result = append(result, []byte(v)...)
				case int:
					result = append(result, []byte(strconv.Itoa(v))...)
				case int64:
					result = append(result, []byte(strconv.FormatInt(v, 10))...)
				case int32:
					result = append(result, []byte(strconv.FormatInt(int64(v), 10))...)
				case float64:
					result = append(result, strconv.AppendFloat(buf[:0], float64(v), 'f', -1, 64)...)
				case float32:
					result = append(result, strconv.AppendFloat(buf[:0], float64(v), 'f', -1, 64)...)
				case bool:
					result = append(result, []byte(strconv.FormatBool(v))...)
				case []int:
					result = append(result, []byte(IntSliceToString(v, " "))...)
				case []int64:
					result = append(result, []byte(IntSliceToString(v, " "))...)
				case []int32:
					result = append(result, []byte(IntSliceToString(v, " "))...)
				case []string:
					result = append(result, []byte(StringSliceToString(v, " "))...)
				case []float32:
					result = append(result, []byte(FloatSliceToString(v, " ", -1))...)
				case []float64:
					result = append(result, []byte(FloatSliceToString(v, " ", -1))...)
				default:
					// 如果类型不匹配，保留原始占位符
					result = append(result, []byte(fmt.Sprintf("{{%s}}", key))...)
				}
			} else {
				// 如果没有找到对应的值，则保留原始占位符
				result = append(result, template[i:i+end+4]...)
			}
			i += end + 2 // 跳过 '}}'
		} else {
			result = append(result, template[i])
			i++
		}
	}
	return result
}

// 快速版本：[]int → string
func IntSliceToString[T int32 | int | int64](slice []T, sep string) string {
	if len(slice) == 0 {
		return ""
	}
	var b strings.Builder
	tmp := make([]byte, 0, 32)

	tmp = strconv.AppendInt(tmp[:0], int64(slice[0]), 10)
	b.Write(tmp)

	for _, v := range slice[1:] {
		b.WriteString(sep)
		tmp = strconv.AppendInt(tmp[:0], int64(v), 10)
		b.Write(tmp)
	}
	return b.String()
}

// 快速版本：[]float64 → string（固定精度）
func FloatSliceToString[T float32 | float64](slice []T, sep string, prec int) string {
	if len(slice) == 0 {
		return ""
	}
	var b strings.Builder
	tmp := make([]byte, 0, 64)

	tmp = strconv.AppendFloat(tmp[:0], float64(slice[0]), 'f', prec, 64)
	b.Write(tmp)

	for _, v := range slice[1:] {
		b.WriteString(sep)
		tmp = strconv.AppendFloat(tmp[:0], float64(v), 'f', prec, 64)
		b.Write(tmp)
	}
	return b.String()
}

// 快速版本：[]string → string
func StringSliceToString(slice []string, sep string) string {
	return strings.Join(slice, sep)
}
