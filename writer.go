package zlg

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/logging"
	"github.com/rs/zerolog"
)

type CloudLoggingWriter struct {
	ctx         context.Context
	wroteOnce   bool
	logger      *logging.Logger
	severityMap map[zerolog.Level]logging.Severity

	OnError func(err error)

	zerolog.LevelWriter
}

// DefaultSeverityMap contains the default zerolog.Level -> logging.Severity mappings.
var DefaultSeverityMap = map[zerolog.Level]logging.Severity{
	zerolog.DebugLevel: logging.Debug,
	zerolog.InfoLevel:  logging.Info,
	zerolog.WarnLevel:  logging.Warning,
	zerolog.ErrorLevel: logging.Error,
	zerolog.PanicLevel: logging.Critical,
	zerolog.FatalLevel: logging.Critical,
}

// secretly, we keep tabs of all loggers
var loggersWeMade = make([]*logging.Logger, 0, 1)

func (c *CloudLoggingWriter) Write(p []byte) (int, error) {
	// writing to stackdriver without levels? o-okay...
	entry := logging.Entry{Payload: json.RawMessage(p)}
	if !c.wroteOnce {
		c.wroteOnce = true
		err := c.logger.LogSync(c.ctx, entry)
		if err != nil {
			return 0, err
		}
	} else {
		c.logger.Log(entry)
	}
	return len(p), nil
}

func (c *CloudLoggingWriter) WriteLevel(level zerolog.Level, payload []byte) (int, error) {
	entry := logging.Entry{
		Severity: c.severityMap[level],
		Payload:  json.RawMessage(payload),
	}
	if !c.wroteOnce {
		c.wroteOnce = true
		err := c.logger.LogSync(c.ctx, entry)
		if err != nil {
			return 0, err
		}
	} else {
		c.logger.Log(entry)
	}
	return len(payload), nil
}

// CloudLoggingOptions specifies some optional configuration.
type CloudLoggingOptions struct {
	// SeverityMap can be optionally specified to use instead of DefaultSeverityMap.
	SeverityMap map[zerolog.Level]logging.Severity

	// Logger can be optionally provided in lieu of constructing a logger on the caller's behalf.
	Logger *logging.Logger

	// LoggerOptions is optionally used to construct a Logger.
	LoggerOptions []logging.LoggerOption
}

// NewCloudLoggingWriter creates a LevelWriter that logs only to GCP Cloud Logging using non-blocking calls.
func NewCloudLoggingWriter(ctx context.Context, projectID, logID string, opts CloudLoggingOptions) (writer *CloudLoggingWriter, err error) {
	logger := opts.Logger
	var client *logging.Client
	if opts.Logger == nil {
		client, err = logging.NewClient(ctx, projectID)
		if err != nil {
			return
		}
		logger = client.Logger(logID, opts.LoggerOptions...)
		loggersWeMade = append(loggersWeMade, logger)
	}
	severityMap := opts.SeverityMap
	if severityMap == nil {
		severityMap = DefaultSeverityMap
	}
	writer = &CloudLoggingWriter{
		ctx:         ctx,
		logger:      logger,
		severityMap: severityMap,
	}
	client.OnError = func(err error) {
		writer.OnError(err)
	}
	return
}

// Flush blocks while flushing all loggers this module created.
func Flush() {
	for _, logger := range loggersWeMade {
		if logger != nil {
			logger.Flush()
		}
	}
}
