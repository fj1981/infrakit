package cyfct

import (
	"errors"
	"reflect"
	"sync"

	"github.com/fj1981/infrakit/pkg/cylog"
)

var (
	creatorMap  = sync.Map{}
	instanceMap = sync.Map{}
	groupMap    = sync.Map{}
)

func getTypeName[T any]() string {
	var t T
	return reflect.TypeOf(t).String()
}

func getInstanceByName(name string) (any, error) {
	if instance, ok := instanceMap.Load(name); ok {
		return instance, nil
	}
	if f, ok := creatorMap.Load(name); ok {
		// Use reflection to call the creator function
		creatorValue := reflect.ValueOf(f)

		// Check if the function type is compatible
		if creatorValue.Kind() != reflect.Func ||
			creatorValue.Type().NumOut() != 2 ||
			!creatorValue.Type().Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
			panic("invalid creator function: " + name)
		}

		// Call the function using reflection
		results := creatorValue.Call(nil)

		// Check for error
		if !results[1].IsNil() {
			// We need to handle the error, but we can't use type assertion
			// Instead, we'll use reflection to check if it's an error type
			errValue := results[1]
			panic(errValue.Interface().(error).Error())
		}

		// Get the instance
		v := results[0].Interface()
		instanceMap.Store(name, v)
		return v, nil
	}
	return nil, errors.New("instance not found")
}
func GetInstance[T any]() (T, error) {
	name := getTypeName[T]()
	instance, err := getInstanceByName(name)
	var r T
	if err != nil {
		cylog.Skip(0).Error("get instance by name failed", "typeName", name, "err", err)
		return r, err
	}
	r, ok := instance.(T)
	if !ok {
		return r, errors.New("instance type mismatch for type: " + name)
	}
	return r, nil
}

func RegisterFactory[T any](creator func() (T, error)) {
	name := getTypeName[T]()
	creatorMap.Store(name, creator)
}

func RegisterFactoryWithGroup[T any](group string, creator func() (T, error)) {
	typeName := getTypeName[T]()
	creatorMap.Store(typeName, creator)
	if s, ok := groupMap.Load(group); ok {
		typeMap := s.(map[string]struct{})
		typeMap[typeName] = struct{}{}
		groupMap.Store(group, typeMap)
	} else {
		groupMap.Store(group, map[string]struct{}{typeName: {}})
	}
}

func GetInstancesByGroup[T any](group string) ([]T, error) {
	r := []T{}
	if s, ok := groupMap.Load(group); ok {
		typeMap, ok := s.(map[string]struct{})
		if !ok {
			return nil, errors.New("invalid group map structure")
		}
		for typeName := range typeMap {
			if instance, err := getInstanceByName(typeName); err == nil {
				rr, ok := instance.(T)
				if !ok {
					return nil, errors.New("instance type mismatch for type: " + typeName)
				}
				r = append(r, rr)
			} else {
				cylog.Skip(0).Error("get instance by name failed", "typeName", typeName, "err", err)
			}
		}
	}
	if len(r) == 0 {
		return nil, errors.New("no instance found for group: " + group)
	}
	return r, nil
}
