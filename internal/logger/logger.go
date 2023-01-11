package logger

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

const messageKey = "message"
const jobBufferSize = 100
const logSinkPathEnvVar = "MONGODB_LOG_PATH"
const maxDocumentLengthEnvVar = "MONGODB_LOG_MAX_DOCUMENT_LENGTH"

// DefaultMaxDocumentLength is the default maximum length of a stringified BSON document in bytes.
const DefaultMaxDocumentLength = 1000

// TruncationSuffix are trailling ellipsis "..." appended to a message to indicate to the user that truncation occurred.
// This constant does not count toward the max document length.
const TruncationSuffix = "..."

// LogSink represents a logging implementation. It is specifically designed to be a subset of go-logr/logr's LogSink
// interface.
type LogSink interface {
	Info(int, string, ...interface{})
}

type job struct {
	level Level
	msg   ComponentMessage
}

// Logger is the driver's logger. It is used to log messages from the driver either to OS or to a custom LogSink.
type Logger struct {
	ComponentLevels   map[Component]Level
	Sink              LogSink
	MaxDocumentLength uint

	jobs chan job
}

// New will construct a new logger with the given LogSink. If the given LogSink is nil, then the logger will log using
// the standard library.
//
// If the given LogSink is nil, then the logger will log using the standard library with output to os.Stderr.
//
// The "componentLevels" parameter is variadic with the latest value taking precedence. If no component has a LogLevel
// set, then the constructor will attempt to source the LogLevel from the environment.
func New(sink LogSink, maxDocumentLength uint, componentLevels map[Component]Level) *Logger {
	return &Logger{
		ComponentLevels: selectComponentLevels(
			func() map[Component]Level { return componentLevels },
			getEnvComponentLevels,
		),

		MaxDocumentLength: selectMaxDocumentLength(
			func() uint { return maxDocumentLength },
			getEnvMaxDocumentLength,
		),

		Sink: selectLogSink(
			func() LogSink { return sink },
			getEnvLogSink,
		),

		jobs: make(chan job, jobBufferSize),
	}

}

// Close will close the logger and stop the printer goroutine.
func (logger Logger) Close() {
	close(logger.jobs)
}

// Is will return true if the given LogLevel is enabled for the given LogComponent.
func (logger Logger) Is(level Level, component Component) bool {
	return logger.ComponentLevels[component] >= level
}

// TODO: (GODRIVER-2570) add an explanation
func (logger *Logger) Print(level Level, msg ComponentMessage) {
	select {
	case logger.jobs <- job{level, msg}:
	default:
		logger.jobs <- job{level, &CommandMessageDropped{}}
	}
}

// StartPrintListener will start a goroutine that will listen for log messages and attempt to print them to the
// configured LogSink.
func StartPrintListener(logger *Logger) {
	go func() {
		for job := range logger.jobs {
			level := job.level
			levelInt := int(level)

			msg := job.msg

			// If the level is not enabled for the component, then skip the message.
			if !logger.Is(level, msg.Component()) {
				return
			}

			sink := logger.Sink

			// If the sink is nil, then skip the message.
			if sink == nil {
				return
			}

			keysAndValues, err := formatMessage(msg.Serialize(), logger.MaxDocumentLength)
			if err != nil {
				sink.Info(levelInt, "error parsing keys and values from BSON message: %v", err)

			}

			sink.Info(levelInt-DiffToInfo, msg.Message(), keysAndValues...)
		}
	}()
}

func truncate(str string, width uint) string {
	if len(str) <= int(width) {
		return str
	}

	// Truncate the byte slice of the string to the given width.
	newStr := str[:width]

	// Check if the last byte is at the beginning of a multi-byte character.
	// If it is, then remove the last byte.
	if newStr[len(newStr)-1]&0xC0 == 0xC0 {
		return newStr[:len(newStr)-1]
	}

	// Check if the last byte is in the middle of a multi-byte character. If it is, then step back until we
	// find the beginning of the character.
	if newStr[len(newStr)-1]&0xC0 == 0x80 {
		for i := len(newStr) - 1; i >= 0; i-- {
			if newStr[i]&0xC0 == 0xC0 {
				return newStr[:i]
			}
		}
	}

	return newStr + TruncationSuffix
}

// TODO: (GODRIVER-2570) remove magic strings from this function. These strings could probably go into internal/const.go
func formatMessage(keysAndValues []interface{}, commandWidth uint) ([]interface{}, error) {
	formattedKeysAndValues := make([]interface{}, len(keysAndValues))
	for i := 0; i < len(keysAndValues); i += 2 {
		key := keysAndValues[i].(string)
		val := keysAndValues[i+1]

		switch key {
		case "command", "reply":
			// Command should be a bson.Raw value.
			raw, ok := val.(bson.Raw)
			if !ok {
				return nil, fmt.Errorf("expected value for key %q to be a bson.Raw, but got %T",
					key, val)
			}

			str := raw.String()
			if len(str) == 0 {
				val = bson.RawValue{
					Type:  bsontype.EmbeddedDocument,
					Value: []byte{0x05, 0x00, 0x00, 0x00, 0x00},
				}.String()
			} else {
				val = truncate(str, commandWidth)
			}

		}

		formattedKeysAndValues[i] = key
		formattedKeysAndValues[i+1] = val
	}

	return formattedKeysAndValues, nil
}

// getEnvMaxDocumentLength will attempt to get the value of "MONGODB_LOG_MAX_DOCUMENT_LENGTH" from the environment, and
// then parse it as an unsigned integer. If the environment variable is not set, then this function will return 0.
func getEnvMaxDocumentLength() uint {
	max := os.Getenv(maxDocumentLengthEnvVar)
	if max == "" {
		return 0
	}

	maxUint, err := strconv.ParseUint(max, 10, 32)
	if err != nil {
		return 0
	}

	return uint(maxUint)
}

// selectMaxDocumentLength will return the first non-zero result of the getter functions.
func selectMaxDocumentLength(getLen ...func() uint) uint {
	for _, get := range getLen {
		if len := get(); len != 0 {
			return len
		}
	}

	return DefaultMaxDocumentLength
}

type logSinkPath string

const (
	logSinkPathStdOut logSinkPath = "stdout"
	logSinkPathStdErr logSinkPath = "stderr"
)

// getEnvLogsink will check the environment for LogSink specifications. If none are found, then a LogSink with an stderr
// writer will be returned.
func getEnvLogSink() LogSink {
	path := os.Getenv(logSinkPathEnvVar)
	lowerPath := strings.ToLower(path)

	if lowerPath == string(logSinkPathStdErr) {
		return newOSSink(os.Stderr)
	}

	if lowerPath == string(logSinkPathStdOut) {
		return newOSSink(os.Stdout)
	}

	if path != "" {
		return newOSSink(os.NewFile(uintptr(syscall.Stdout), path))
	}

	return nil
}

// selectLogSink will select the first non-nil LogSink from the given LogSinks.
func selectLogSink(getSink ...func() LogSink) LogSink {
	for _, getSink := range getSink {
		if sink := getSink(); sink != nil {
			return sink
		}
	}

	return newOSSink(os.Stderr)
}

// getEnvComponentLevels returns a component-to-level mapping defined by the environment variables, with
// "MONGODB_LOG_ALL" taking priority.
func getEnvComponentLevels() map[Component]Level {
	componentLevels := make(map[Component]Level)
	globalLevel := parseLevel(os.Getenv(string(componentEnvVarAll)))

	for _, envVar := range allComponentEnvVars {
		if envVar == componentEnvVarAll {
			continue
		}

		level := globalLevel
		if globalLevel == OffLevel {
			level = parseLevel(os.Getenv(string(envVar)))
		}

		componentLevels[envVar.component()] = level
	}

	return componentLevels
}

// selectComponentLevels returns a new map of LogComponents to LogLevels that is the result of merging the provided
// maps. The maps are merged in order, with the earlier maps taking priority.
func selectComponentLevels(getters ...func() map[Component]Level) map[Component]Level {
	selected := make(map[Component]Level)
	set := make(map[Component]struct{})

	for _, getComponentLevels := range getters {
		for component, level := range getComponentLevels() {
			if _, ok := set[component]; !ok {
				selected[component] = level
			}

			set[component] = struct{}{}
		}
	}

	return selected
}
