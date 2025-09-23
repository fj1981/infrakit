package cyutil

import (
	"strings"
)

// DelimiterInfo 用于存储找到的分隔符信息
type DelimiterInfo struct {
	S string // 分隔符的内容
	P int    // 分隔符在原字符串中的起始位置（索引）
}

// FindDelimiters 查找字符串 str 中所有由 sep 组成的连续分隔符
// 返回包含分隔符内容和起始位置的 DelimiterInfo 切片
func FindDelimiters(str, sep string) []DelimiterInfo {
	var result []DelimiterInfo

	if sep == "" {
		return result // 如果分隔符为空，直接返回空切片
	}

	start := 0
	for start < len(str) {
		// 查找下一个 sep 的位置
		index := strings.Index(str[start:], sep)
		if index == -1 {
			break // 没有找到更多分隔符，退出循环
		}

		// 计算实际在原字符串中的位置
		pos := start + index

		// 找到连续的 sep 的长度
		delimiterEnd := pos
		for delimiterEnd < len(str) && strings.HasPrefix(str[delimiterEnd:], sep) {
			delimiterEnd += len(sep)
		}

		// 提取完整的连续分隔符
		fullDelimiter := str[pos:delimiterEnd]

		// 添加到结果中
		result = append(result, DelimiterInfo{
			S: fullDelimiter,
			P: pos,
		})

		// 从连续分隔符结束后继续查找
		start = delimiterEnd
	}

	return result
}

func ReplaceSqlStringContent(line string, expectedEnd string, vPlaceholder ...string) (processedLine string, nextExpectedEnd string) {
	placeholder := "___STR___"
	if len(vPlaceholder) > 0 {
		placeholder = vPlaceholder[0]
	}
	var result strings.Builder
	i := 0
	marks := []string{"##", "@@", "$$", "**", "??"}
	mark := ""
	for _, mark = range marks {
		if strings.Contains(line, mark) {
			continue
		}
		line = strings.ReplaceAll(line, "''", mark)
		break
	}
	nextExpectedEnd = expectedEnd
	for i < len(line) {
		restring := line[i:]
		if nextExpectedEnd != "" {
			if nextExpectedEnd != "'" {
				nextExpectedEnd += "'"
			}
			i2 := strings.Index(restring, nextExpectedEnd)
			if i2 == -1 {
				result.WriteString(placeholder)
				break
			}
			result.WriteString(placeholder)
			i2 += len(nextExpectedEnd)
			i += i2
			nextExpectedEnd = ""
			continue
		}
		i2 := strings.Index(restring, "'")
		if i2 == -1 {
			result.WriteString(restring)
			break
		} else {
			i += i2
			if i == 0 {
				nextExpectedEnd = "'"
			} else if i >= 1 {
				if line[i-1] == 'q' {
					if i2+1 < len(restring) {
						if i >= 2 {
							if !isWhitespace(line[i-2]) {
								result.WriteString(restring[:i2+1])
								i++
								continue
							}
						}
						result.WriteString(restring[:i2-1])
						nextExpectedEnd = string(getCloseDelimiter(restring[i2+1]))
					} else {
						result.WriteString(restring[:i2+1])
						nextExpectedEnd = ""
					}
				} else {
					result.WriteString(restring[:i2])
					nextExpectedEnd = "'"
				}
			}
			i++
		}
	}
	return strings.ReplaceAll(result.String(), mark, "''"), nextExpectedEnd
}

func getCloseDelimiter(openDelim byte) byte {
	switch openDelim {
	case '{':
		return '}'
	case '[':
		return ']'
	case '(':
		return ')'
	case '<':
		return '>'
	default:
		return openDelim
	}
}

func isWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func RemoveSqlComments(line string, inMultiLineComment bool) (processedLine string, stillInComment bool) {
	if inMultiLineComment {
		endPos := strings.Index(line, "*/")
		if endPos == -1 {
			return "", true
		}
		return line[endPos+2:], false
	}
	result := line
	i := 0
	for i < len(result) {
		if i+1 < len(result) && result[i] == '-' && result[i+1] == '-' {
			result = result[:i]
			result = result[:i]
			break
		}

		if i+1 < len(result) && result[i] == '/' && result[i+1] == '*' {
			endPos := strings.Index(result[i+2:], "*/")
			if endPos == -1 {
				result = result[:i]
				return result, true
			}
			result = result[:i] + result[i+2+endPos+2:]
			continue
		}

		i++
	}

	return result, false
}

func PadStart(s string, length int, pad string) string {
	// 处理空字符串和空填充字符的情况
	if s == "" {
		return strings.Repeat(pad, length)
	}
	if pad == "" {
		pad = " "
	}

	// 将字符串和填充字符转换为rune切片
	sRunes := []rune(s)
	padRunes := []rune(pad)
	if len(padRunes) == 0 {
		padRunes = []rune{' '}
	}

	// 计算字符串的视觉宽度
	visualWidth := 0
	for _, r := range sRunes {
		if r < 128 { // ASCII字符
			visualWidth++
		} else if r == '\u00b5' || r == 'µ' { // 微符号特殊处理
			visualWidth++
		} else if r > 0x2E80 && r < 0xFE4F { // 中日韩文字范围
			visualWidth += 2
		} else {
			visualWidth++
		}
	}

	// 如果字符串视觉宽度已经大于或等于目标长度，直接返回
	if visualWidth >= length {
		return s
	}

	// 计算需要填充的数量
	padCount := length - visualWidth

	// 构建填充字符串
	var result []rune
	padIndex := 0
	for i := 0; i < padCount; i++ {
		result = append(result, padRunes[padIndex])
		padIndex = (padIndex + 1) % len(padRunes) // 循环使用填充字符
	}

	// 添加原始字符串
	result = append(result, sRunes...)

	return string(result)
}
