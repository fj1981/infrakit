package cygin

import (
	"errors"
	"reflect"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/locales/en"
	"github.com/go-playground/locales/zh"
	"github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	en_translations "github.com/go-playground/validator/v10/translations/en"
	zh_translations "github.com/go-playground/validator/v10/translations/zh"
)

// 支持的语言
const (
	LangEN = "en"
	LangZH = "zh"
)

var (
	once       sync.Once
	validate   *validator.Validate
	translator *ut.UniversalTranslator
)

// GetValidator 返回全局验证器实例
func GetValidator() *validator.Validate {
	once.Do(func() {
		validate = validator.New()

		// 初始化翻译器
		setupTranslator()

		// 使用 json tag 名称作为错误字段名
		validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
			name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
			if name == "-" {
				return ""
			}
			return name
		})

		// 这里可以注册自定义验证器
		// 例如: validate.RegisterValidation("customtag", customTagFunc)
	})
	return validate
}

// setupTranslator 初始化翻译器
func setupTranslator() {
	// 创建英文和中文的locale
	enLocale := en.New()
	zhLocale := zh.New()

	// 创建通用翻译器，英文为默认语言
	translator = ut.New(enLocale, enLocale, zhLocale)

	// 注册英文翻译
	enTrans, _ := translator.GetTranslator(LangEN)
	en_translations.RegisterDefaultTranslations(validate, enTrans)

	// 注册中文翻译
	zhTrans, _ := translator.GetTranslator(LangZH)
	zh_translations.RegisterDefaultTranslations(validate, zhTrans)
}

// getLanguageFromContext 从 Gin Context 中获取语言偏好
func getLanguageFromContext(c *gin.Context) string {
	// 优先从 Accept-Language 头部获取
	if acceptLang := c.GetHeader("Accept-Language"); acceptLang != "" {
		// 简单解析 Accept-Language 头部
		if strings.Contains(strings.ToLower(acceptLang), "zh") {
			return LangZH
		}
	}

	// 从查询参数获取
	if lang := c.Query("lang"); lang != "" {
		if lang == LangZH || lang == LangEN {
			return lang
		}
	}

	// 默认返回英文
	return LangEN
}

// translateValidationErrors 使用官方翻译器翻译验证错误
func translateValidationErrors(errs validator.ValidationErrors, lang string) []string {
	trans, found := translator.GetTranslator(lang)
	if !found {
		// 如果没有找到对应语言的翻译器，使用英文
		trans, _ = translator.GetTranslator(LangEN)
	}

	var messages []string
	for _, err := range errs {
		messages = append(messages, err.Translate(trans))
	}
	return messages
}

// SetupValidator 设置 Gin 使用 go-playground/validator/v10
func SetupValidator() {
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		// 如果 Gin 已经使用了 go-playground/validator/v10，我们可以直接扩展它
		validate = v

		// 初始化翻译器
		setupTranslator()

		// 使用 json tag 名称作为错误字段名
		validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
			name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
			if name == "-" {
				return ""
			}
			return name
		})

		// 这里可以注册自定义验证器
	} else {
		// 如果 Gin 不是使用 go-playground/validator/v10，我们需要替换它
		// 但这种情况在 Gin 最新版本中不太可能发生，因为 Gin 默认使用 go-playground/validator
		GetValidator()
	}
}

// ValidateStruct 验证结构体
func ValidateStruct(obj any) error {
	return GetValidator().Struct(obj)
}

// ValidateStructWithLang 验证结构体并返回国际化错误消息
func ValidateStructWithLang(obj any, lang string) error {
	err := GetValidator().Struct(obj)
	if err != nil {
		if validationErrors, ok := err.(validator.ValidationErrors); ok {
			messages := translateValidationErrors(validationErrors, lang)
			return errors.New(strings.Join(messages, "; "))
		}
	}
	return err
}

// BindAndValidate 绑定请求并验证
func BindAndValidate(c *gin.Context, obj interface{}) error {
	// 根据请求类型绑定
	if err := c.ShouldBind(obj); err != nil {
		return err
	}

	// 验证结构体
	return ValidateStructWithLang(obj, getLanguageFromContext(c))
}

// BindQueryAndValidate 绑定查询参数并验证
func BindQueryAndValidate(c *gin.Context, obj interface{}) error {
	if err := c.ShouldBindQuery(obj); err != nil {
		return err
	}
	return ValidateStructWithLang(obj, getLanguageFromContext(c))
}

// BindJSONAndValidate 绑定 JSON 并验证
func BindJSONAndValidate(c *gin.Context, obj interface{}) error {
	if err := c.ShouldBindJSON(obj); err != nil {
		return err
	}
	return ValidateStructWithLang(obj, getLanguageFromContext(c))
}
