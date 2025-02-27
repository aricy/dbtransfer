package i18n

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

var (
	supportsChinese bool
	once            sync.Once
	currentLang     string
)

func init() {
	// 检测系统是否支持中文显示
	if IsChinese() {
		currentLang = "zh"
	} else {
		currentLang = "en"
	}
	logrus.Debugf("系统检测到的默认语言: %s", currentLang)
}

// IsChinese 检测系统是否支持中文显示
func IsChinese() bool {
	once.Do(func() {
		// 检测方法取决于操作系统
		switch runtime.GOOS {
		case "windows":
			// 在Windows上检查是否安装了中文语言包
			supportsChinese = checkWindowsChineseSupport()
		case "darwin", "linux":
			// 在macOS和Linux上检查locale设置
			supportsChinese = checkUnixChineseSupport()
		default:
			// 默认假设不支持中文
			supportsChinese = false
		}
		logrus.Debugf("系统中文支持检测结果: %v", supportsChinese)
	})
	return supportsChinese
}

// 在Windows上检查中文支持
func checkWindowsChineseSupport() bool {
	// 检查环境变量
	if strings.Contains(os.Getenv("LANG"), "zh") ||
		strings.Contains(os.Getenv("LC_ALL"), "zh") {
		return true
	}

	// 检查是否使用UTF-8或其他支持中文的编码
	if strings.Contains(strings.ToLower(os.Getenv("LANG")), "utf-8") ||
		strings.Contains(strings.ToLower(os.Getenv("LANG")), "utf8") ||
		strings.Contains(strings.ToLower(os.Getenv("LC_ALL")), "utf-8") ||
		strings.Contains(strings.ToLower(os.Getenv("LC_ALL")), "utf8") {
		return true
	}

	// 尝试执行命令检查中文代码页
	cmd := exec.Command("chcp")
	output, err := cmd.Output()
	if err == nil {
		// 检查输出是否包含中文代码页 (936 = GBK/GB2312, 54936 = GB18030)
		return strings.Contains(string(output), "936") || strings.Contains(string(output), "54936")
	}

	// 检查Windows的代码页是否支持中文
	cmd = exec.Command("powershell", "-Command", "[System.Text.Encoding]::Default.CodePage")
	output, err = cmd.Output()
	if err == nil {
		// 65001是UTF-8编码，936是GBK编码，950是Big5编码
		return strings.Contains(string(output), "65001") ||
			strings.Contains(string(output), "936") ||
			strings.Contains(string(output), "950")
	}

	return false
}

// 在Unix系统上检查中文支持
func checkUnixChineseSupport() bool {
	// 检查环境变量
	for _, env := range []string{"LANG", "LC_ALL", "LC_CTYPE"} {
		if val := os.Getenv(env); strings.Contains(strings.ToLower(val), "zh") ||
			strings.Contains(strings.ToLower(val), "utf-8") ||
			strings.Contains(strings.ToLower(val), "utf8") {
			return true
		}
	}

	// 检查是否使用其他支持中文的编码
	for _, env := range []string{"LANG", "LC_ALL", "LC_CTYPE"} {
		val := strings.ToLower(os.Getenv(env))
		// 检查常见的支持中文的编码
		if strings.Contains(val, "gb") || // GB2312, GBK, GB18030
			strings.Contains(val, "big5") || // Big5
			strings.Contains(val, "euc-cn") || // EUC-CN
			strings.Contains(val, "euc-tw") { // EUC-TW
			return true
		}
	}

	// 尝试执行locale命令
	cmd := exec.Command("locale")
	output, err := cmd.Output()
	if err == nil {
		outputLower := strings.ToLower(string(output))
		return strings.Contains(outputLower, "zh") ||
			strings.Contains(outputLower, "utf-8") ||
			strings.Contains(outputLower, "utf8") ||
			strings.Contains(outputLower, "gb") || // GB2312, GBK, GB18030
			strings.Contains(outputLower, "big5") || // Big5
			strings.Contains(outputLower, "euc-cn") || // EUC-CN
			strings.Contains(outputLower, "euc-tw") // EUC-TW
	}

	return false
}

// SetLanguage 设置当前语言
func SetLanguage(lang string) {
	if lang == "zh" || lang == "en" {
		currentLang = lang
	} else if lang != "" {
		logrus.Warnf(Tr("不支持的语言: %s, 将根据系统环境选择语言", "Unsupported language: %s, will select language based on system environment"), lang)
		// 如果指定了不支持的语言，则根据系统支持情况选择默认语言
		if IsChinese() {
			currentLang = "zh"
		} else {
			currentLang = "en"
		}
	}
}

// GetLanguage 获取当前语言
func GetLanguage() string {
	return currentLang
}

// Tr 翻译文本
func Tr(zhText, enText string, args ...interface{}) string {
	var text string

	// 如果用户明确设置了语言，则使用设置的语言
	if currentLang == "zh" {
		text = zhText
	} else if currentLang == "en" {
		text = enText
	} else {
		// 如果没有明确设置语言，则根据系统环境自动选择
		if IsChinese() {
			text = zhText
		} else {
			text = enText
		}
	}

	// 处理格式化参数
	if len(args) > 0 {
		return fmt.Sprintf(text, args...)
	}
	return text
}

// 创建一个自定义的日志格式化器
type I18nFormatter struct {
	TimestampFormat string
}

// 定义日志级别颜色
var levelColors = map[logrus.Level]string{
	logrus.DebugLevel: "\033[36m", // 青色
	logrus.InfoLevel:  "\033[32m", // 绿色
	logrus.WarnLevel:  "\033[33m", // 黄色
	logrus.ErrorLevel: "\033[31m", // 红色
	logrus.FatalLevel: "\033[35m", // 紫色
	logrus.PanicLevel: "\033[31m", // 红色
}

// 定义颜色重置代码
const colorReset = "\033[0m"

// Format 实现logrus.Formatter接口
func (f *I18nFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	// 获取时间戳
	timestamp := entry.Time.Format("2006-01-02 15:04:05")

	// 获取日志级别（大写）
	levelText := strings.ToUpper(entry.Level.String())

	// 获取颜色代码
	color := levelColors[entry.Level]

	// 构建日志消息
	msg := fmt.Sprintf("[%s] %s%s%s %s\n",
		timestamp,
		color,
		levelText,
		colorReset,
		entry.Message)

	return []byte(msg), nil
}

// InitLogger 初始化日志系统
func InitLogger() {
	// 设置日志格式化器
	logrus.SetFormatter(&I18nFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
	})

	// 设置日志级别
	logrus.SetLevel(logrus.InfoLevel)
}
