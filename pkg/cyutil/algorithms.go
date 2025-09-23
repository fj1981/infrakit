package cyutil

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

func GraphSort(names []string, depGraph map[string]map[string]struct{}) ([]string, error) {
	sortedTables := []string{}

	for _, table := range names {
		if _, exists := depGraph[table]; !exists {
			sortedTables = append(sortedTables, table)
		}
	}

	for len(depGraph) > 0 {

		addSort := []string{}
		for table, deps := range depGraph {
			for _, st := range sortedTables {
				delete(deps, st)
			}
			if len(deps) == 0 {
				addSort = append(addSort, table)
				break // Break and restart the outer loop with the updated sortedTables
			}
		}

		// Detect cycles - if no progress was made in this iteration
		if len(addSort) == 0 {
			return nil, errors.New("circular dependency detected")
		}
		sortedTables = append(sortedTables, addSort...)
		for _, table := range addSort {
			delete(depGraph, table)
		}
	}
	return sortedTables, nil
}

type WordInfo struct {
	Word      string
	Pos       int
	Separator string
	Index     int
}

type CallbackFunc func(w *WordInfo) error

// traverseString tokenizes a string and calls a callback for each word found.
func TraverseString(s string, separatorChars string, callback CallbackFunc) error {
	isSeparator := func(r rune) bool {
		return unicode.IsSpace(r) || strings.ContainsRune(separatorChars, r)
	}

	i := 0
	index := 0
	for i < len(s) {
		// 1. Skip leading separators
		start := i
		for start < len(s) && isSeparator(rune(s[start])) {
			start++
		}

		// 2. Find the end of the next word
		end := start
		for end < len(s) && !isSeparator(rune(s[end])) {
			end++
		}

		// If we found a word, process it
		if end > start {
			word := s[start:end]

			// 3. Find the end of the separators that follow the word
			sepEnd := end
			for sepEnd < len(s) && isSeparator(rune(s[sepEnd])) {
				sepEnd++
			}
			separators := s[end:sepEnd]
			if err := callback(&WordInfo{
				Word:      word,
				Pos:       start,
				Separator: separators,
				Index:     index,
			}); err != nil {
				return err
			}
			// 4. Move index past the word and its separators
			i = sepEnd
			index++
		} else {
			// No word found, we are at the end of the string
			break
		}
	}
	return nil
}

func TravelMap(m map[string]any, f func(k string, v any) error) error {
	for key, value := range m {
		err := f(key, value)
		if err != nil {
			return err
		}
		if subItems, ok := value.([]any); ok {
			for _, item := range subItems {
				if subMap, ok := item.(map[string]any); ok {
					err = TravelMap(subMap, f)
					if err != nil {
						return err
					}
				}
			}
		}

		if subMaps, ok := value.([]map[string]any); ok {
			for _, subMap := range subMaps {
				err = TravelMap(subMap, f)
				if err != nil {
				}
			}
		}

		// 检查当前值是否也是一个 map，如果是，则递归调用 TravelMap
		if subMap, ok := value.(map[string]any); ok {
			err = TravelMap(subMap, f)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func WalkMapLeaves(root map[string]any, f func(path string, v any) error) error {
	var walk func(prefix string, v any) error

	walk = func(prefix string, v any) error {
		switch node := v.(type) {
		case map[string]any:
			for k, sub := range node {
				newPath := k
				if prefix != "" {
					newPath = prefix + "." + k
				}
				if err := walk(newPath, sub); err != nil {
					return err
				}
			}
		case []any:
			for i, sub := range node {
				newPath := fmt.Sprintf("%s[%d]", prefix, i)
				if err := walk(newPath, sub); err != nil {
					return err
				}
			}
		default:
			// 叶子
			return f(prefix, v)
		}
		return nil
	}

	return walk("", root)
}

func SetValue(m map[string]interface{}, keys []string, value interface{}) (map[string]interface{}, error) {
	if m == nil {
		m = make(map[string]interface{})
	}
	if len(keys) == 0 {
		return m, nil
	}

	currentMap := m
	for i, key := range keys {
		if i == len(keys)-1 {
			// Last key, set the value
			currentMap[key] = value
			break
		}

		next, exists := currentMap[key]
		if !exists || next == nil {
			// Key doesn't exist or is nil, create a new map
			newMap := make(map[string]interface{})
			currentMap[key] = newMap
			currentMap = newMap
			continue
		}

		if nextMap, ok := next.(map[string]interface{}); ok {
			// Key exists and is a map, move to the next level
			currentMap = nextMap
		} else {
			// Key exists but is not a map
			return nil, fmt.Errorf("key '%s' does not point to a map", key)
		}
	}

	return m, nil
}

func GetValue(m map[string]interface{}, keys []string, ignoreCase ...bool) (interface{}, error) {
	if len(keys) == 0 {
		return m, nil
	}

	currentKey := keys[0]
	remainingKeys := keys[1:]
	useIgnoreCase := len(ignoreCase) > 0 && ignoreCase[0]

	var nextMap map[string]interface{}
	var foundValue interface{}
	var keyFound bool

	if useIgnoreCase {
		for k, v := range m {
			if strings.EqualFold(k, currentKey) {
				foundValue = v
				keyFound = true
				break
			}
		}
	} else {
		foundValue, keyFound = m[currentKey]
	}

	if !keyFound {
		return nil, fmt.Errorf("key '%s' not found", currentKey)
	}

	if len(remainingKeys) == 0 {
		return foundValue, nil
	}

	var ok bool
	nextMap, ok = foundValue.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("value for key '%s' is not a map", currentKey)
	}

	return GetValue(nextMap, remainingKeys, ignoreCase...)
}

func Diff(oldDatas, newDatas []map[string]interface{}, key string) (newData, updateData, delData []map[string]interface{}) {
	set := map[string]string{}
	for _, v := range oldDatas {
		key1 := ToStr(v[key])
		md5 := MD5(v)
		set[key1] = md5
	}
	for _, v := range newDatas {
		key2 := ToStr(v[key])
		if m2, ok := set[key2]; ok {
			md5 := MD5(v)
			if m2 != md5 {
				updateData = append(updateData, v)
			}
			delete(set, key2)
		} else {
			newData = append(newData, v)
		}
	}

	for _, v := range oldDatas {
		key1 := ToStr(v[key])
		if _, ok := set[key1]; ok {
			delData = append(delData, v)
		}
	}
	return
}
