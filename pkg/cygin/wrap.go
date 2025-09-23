package cygin

import (
	"errors"
	"mime/multipart"
	"net/http"
	"reflect"

	"github.com/fj1981/infrakit/pkg/cyswag"
	"github.com/fj1981/infrakit/pkg/cyutil"
	"github.com/gin-gonic/gin"
)

type BindType = int

const (
	BindTypeNone   BindType = 0
	BindTypeUri    BindType = 1 << 1
	BindTypeQuery  BindType = 1 << 2
	BindTypeForm   BindType = 1 << 3
	BindTypeJson   BindType = 1 << 4
	BindTypeHeader BindType = 1 << 5
	BindTypeFile   BindType = 1 << 6
	BindTypeAll    BindType = BindTypeUri | BindTypeQuery | BindTypeForm | BindTypeJson | BindTypeHeader | BindTypeFile
)

func getBindFlagsFromValue(v any) (BindType, *cyswag.StructParams) {
	fields := cyswag.ParseParameters(v)
	flags := BindTypeNone
	for _, field := range fields.Params {
		switch field.In {
		case "path":
			flags |= BindTypeUri
		case "query":
			flags |= BindTypeQuery
		case "body":
			flags |= BindTypeJson
		case "formData":
			flags |= BindTypeForm
		case "header":
			flags |= BindTypeHeader
		case "file":
			flags |= BindTypeFile
		}
	}
	return flags, fields
}

// BindAll 尝试从多个来源绑定数据到结构体
// 支持: URI, Query/Form, JSON, Header, File
func BindAll(c *gin.Context, obj any, bindType BindType) error {
	if bindType&BindTypeUri != 0 {
		c.ShouldBindUri(obj)
	}
	if bindType&BindTypeQuery != 0 {
		c.ShouldBindQuery(obj)
	}
	if bindType&BindTypeJson != 0 || bindType&BindTypeForm != 0 {
		c.ShouldBind(obj)
	}
	if bindType&BindTypeHeader != 0 {
		bindHeader(c, obj)
	}
	if bindType&BindTypeFile != 0 {
		bindFile(c, obj)
	}
	if obj != nil {
		return ValidateStructWithLang(obj, getLanguageFromContext(c))
	}
	return nil
}

// bindStructWithTag 通用的结构体字段绑定函数
func bindStructWithTag(obj any, tagName string,
	processor func(field reflect.StructField, fieldValue reflect.Value, tagValue string) error) error {
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)

	// 确保是指针
	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return nil
	}

	t = t.Elem()
	v = v.Elem()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tagValue := field.Tag.Get(tagName)
		if tagValue == "" {
			continue
		}

		fieldValue := v.Field(i)
		if fieldValue.CanSet() {
			if err := processor(field, fieldValue, tagValue); err != nil {
				// 处理错误，这里选择忽略错误继续处理其他字段
				continue
			}
		}
	}
	return nil
}

// bindHeader 使用反射绑定 Header
func bindHeader(c *gin.Context, obj any) error {
	return bindStructWithTag(obj, "header", func(field reflect.StructField, fieldValue reflect.Value, tagValue string) error {
		value := c.GetHeader(tagValue)
		if value == "" {
			return nil
		}

		switch fieldValue.Kind() {
		case reflect.String:
			fieldValue.SetString(value)
		case reflect.Int, reflect.Int32, reflect.Int64:
			fieldValue.SetInt(cyutil.ToInt64(value))
			// 可扩展：使用 strconv.ParseInt
		// 其他类型可继续扩展
		default:
			// 不支持的类型跳过
		}
		return nil
	})
}

// bindFile 使用反射绑定文件（*multipart.FileHeader）
func bindFile(c *gin.Context, obj any) error {
	return bindStructWithTag(obj, "file", func(field reflect.StructField, fieldValue reflect.Value, tagValue string) error {
		// 获取上传的文件
		file, header, err := c.Request.FormFile(tagValue)
		if err != nil {
			return err // 文件可选，错误会被忽略
		}
		defer file.Close()

		if fieldValue.Kind() == reflect.Ptr &&
			fieldValue.Type().Elem() == reflect.TypeOf(multipart.FileHeader{}) {
			fieldValue.Set(reflect.ValueOf(header))
		}
		return nil
	})
}

/* ---------- 统一封装 ---------- */

type PageData struct {
	Data  any `json:"data"`
	Page  int `json:"page"`
	Size  int `json:"size"`
	Total int `json:"total"`
}

type Rsp struct {
	Code  int    `json:"code"`
	Msg   string `json:"msg"`
	Page  *int   `json:"page,omitempty"`
	Size  *int   `json:"size,omitempty"`
	Total *int   `json:"total,omitempty"`
	Data  any    `json:"data,omitempty"`
}

var defaultSuccessCode = 0

func SetDefaultSuccessCode(code int) {
	defaultSuccessCode = code
}

// 统一成功返回
func ok(c *gin.Context, data any) {
	switch p := data.(type) {
	case PageData:
		c.JSON(http.StatusOK, Rsp{
			Code:  defaultSuccessCode,
			Msg:   "success",
			Data:  p.Data,
			Page:  &p.Page,
			Size:  &p.Size,
			Total: &p.Total,
		})
	case *PageData:
		if p == nil {
			c.JSON(http.StatusOK, Rsp{
				Code: defaultSuccessCode,
				Msg:  "success",
			})
			return
		}
		c.JSON(http.StatusOK, Rsp{
			Code:  defaultSuccessCode,
			Msg:   "success",
			Data:  p.Data,
			Page:  &p.Page,
			Size:  &p.Size,
			Total: &p.Total,
		})
	default:
		c.JSON(http.StatusOK, Rsp{
			Code: defaultSuccessCode,
			Msg:  "success",
			Data: data,
		})
	}
}

// 统一失败返回
func fail(c *gin.Context, err error) {
	if e, ok := err.(*Error); ok {
		c.JSON(e.Status, gin.H{"code": e.Code, "msg": e.Msg(FromCtx(c)), "detail": e.Details})
		return
	}
	// fallback
	c.JSON(http.StatusOK, gin.H{"code": ErrCodeInternal, "msg": err.Error()})
}

/* ---------- 泛型 Handler ---------- */

type cbZeroIO func(input, output any)

// 通用版本：可返回数据
func Handle[
	Req any,
	Rsp any,
](fn func(c *gin.Context, req Req) (Rsp, error), cb ...cbZeroIO) gin.HandlerFunc {
	reqObj, _ := cyutil.NewObj[Req]()
	bindType, params := getBindFlagsFromValue(reqObj)
	if len(cb) > 0 {
		rsp, _ := cyutil.NewObj[Rsp]()
		cb[0](params, rsp)
	}
	return func(c *gin.Context) {
		req, isPointer := cyutil.NewObj[Req]()
		if any(req) != (struct{}{}) {
			var reqPtr any
			if isPointer {
				reqPtr = req
			} else {
				reqPtr = &req
			}
			if err := BindAll(c, reqPtr, bindType); err != nil {
				fail(c, NewError(ErrCodeInvalidParam).WithDetail(err.Error()))
				return
			}
		}

		resp, err := fn(c, req)
		if err != nil {
			fail(c, err)
			return
		}
		ok(c, resp)
	}
}

func HandleNoResp[Req any](fn func(c *gin.Context, req Req, cb ...cbZeroIO) error) gin.HandlerFunc {
	return Handle(
		func(c *gin.Context, req Req) (struct{}, error) {
			return struct{}{}, fn(c, req)
		},
	)
}

func HandleAny(fn any, cb ...cbZeroIO) (gin.HandlerFunc, error) {
	if rf, ok := fn.(gin.HandlerFunc); ok {
		return rf, nil
	}

	fnVal := reflect.ValueOf(fn)
	fnType := reflect.TypeOf(fn)

	// --- 初始化阶段：做所有反射检查和准备（只执行一次）---

	if fnType == nil || fnType.Kind() != reflect.Func {
		panic("handler: fn must be a function")
	}
	inNum := fnType.NumIn()
	outNum := fnType.NumOut()
	var params *cyswag.StructParams
	var bindType BindType
	var respInstance any
	var reqPtrNewFunc func() reflect.Value
	if inNum <= 2 {
		if inNum != 0 {
			if fnType.In(0) != reflect.TypeOf((*gin.Context)(nil)) {
				return nil, errors.New("handler: first param must be *gin.Context")
			}
		}
		if inNum == 2 {
			reqArgType := fnType.In(1) // 参数类型（必须是 *T）

			// 确保输入参数是指针类型
			if reqArgType.Kind() != reflect.Ptr {
				return nil, errors.New("handler: request parameter must be a pointer type")
			}
			reqPtrNewFunc = func() reflect.Value {
				return reflect.New(reqArgType.Elem())
			}
			bindType, params = getBindFlagsFromValue(reqPtrNewFunc().Interface())
		}
	} else {
		return nil, errors.New("handler: fn must has less than 2 in params")
	}

	if outNum <= 2 {
		if outNum != 0 {
			if fnType.Out(outNum-1) != reflect.TypeOf((*error)(nil)).Elem() {
				return nil, errors.New("handler: last return must be error")
			}
		}
		if outNum == 2 {
			respInstance = reflect.New(fnType.Out(0)).Interface()
		}
	} else {
		return nil, errors.New("handler: fn must has less than 2 out params")
	}

	if len(cb) > 0 {
		cb[0](params, respInstance)
	}

	return func(c *gin.Context) {
		callArgs := []reflect.Value{reflect.ValueOf(c)}
		if reqPtrNewFunc != nil {
			reqPtrVal := reqPtrNewFunc() // *Req
			if err := BindAll(c, reqPtrVal.Interface(), bindType); err != nil {
				fail(c, NewError(ErrCodeInvalidParam).WithDetail("bind body: "+err.Error()))
				return
			}
			callArgs = append(callArgs, reqPtrVal)
		}
		results := fnVal.Call(callArgs)
		rl := len(results)
		if rl == 0 {
			return
		}
		var err error
		if rl >= 1 && !results[rl-1].IsNil() {
			var ok bool
			err, ok = results[rl-1].Interface().(error)
			if !ok {
				fail(c, errors.New("last return must be error"))
				return
			}
		}
		if err != nil {
			fail(c, err)
			return
		}
		switch rl {
		case 2:
			resp := results[0].Interface()
			ok(c, resp)
		case 1:
			ok(c, nil)
		}

	}, nil
}
