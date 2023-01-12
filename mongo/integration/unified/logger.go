package unified

import (
	"fmt"

	"go.mongodb.org/mongo-driver/internal/logger"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// orderedLogMessage is logMessage with a "order" field representing the order
// in which the log message was observed.
type orderedLogMessage struct {
	*logMessage
	order int
}

// Logger is the Sink used to captured log messages for logger verification in
// the unified spec tests.
type Logger struct {
	lastOrder int
	logQueue  chan orderedLogMessage
}

func newLogger(logQueue chan orderedLogMessage) *Logger {
	return &Logger{
		lastOrder: 0,
		logQueue:  logQueue,
	}
}

// Info implements the logger.Sink interface's "Info" method for printing log
// messages.
func (log *Logger) Info(level int, msg string, args ...interface{}) {
	if log.logQueue == nil {
		return
	}

	// Add the Diff back to the level, as there is no need to create a
	// logging offset.
	level = level + logger.DiffToInfo

	logMessage, err := newLogMessage(level, args...)
	if err != nil {
		panic(err)
	}

	// Send the log message to the "orderedLogMessage" channel for
	// validation.
	log.logQueue <- orderedLogMessage{
		order:      log.lastOrder + 1,
		logMessage: logMessage,
	}

	log.lastOrder++
}

// Error implements the logger.Sink interface's "Error" method for printing log
// errors. In this case, if an error occurs we will simply treat it as
// informational.
func (log *Logger) Error(_ error, msg string, args ...interface{}) {
	log.Info(int(logger.LevelInfo), msg, args)
}

// setLoggerClientOptions sets the logger options for the client entity using
// client options and the observeLogMessages configuration.
func setLoggerClientOptions(entity *clientEntity, clientOptions *options.ClientOptions, olm *observeLogMessages) error {
	if olm == nil {
		return fmt.Errorf("observeLogMessages is nil")
	}

	wrap := func(str string) options.LogLevel {
		return options.LogLevel(logger.ParseLevel(str))
	}

	loggerOpts := options.Logger().SetSink(newLogger(entity.logQueue)).
		SetComponentLevel(options.LogComponentCommand, wrap(olm.Command)).
		SetComponentLevel(options.LogComponentTopology, wrap(olm.Topology)).
		SetComponentLevel(options.LogComponentServerSelection, wrap(olm.ServerSelection)).
		SetComponentLevel(options.LogComponentconnection, wrap(olm.Connection))

	clientOptions.SetLoggerOptions(loggerOpts)

	return nil
}
