package shardctrler

import (
	"encoding/json"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"net/http"
	_ "net/http/pprof"
)

type MyLogger struct {
	logger *zap.Logger
}

var (
	logger MyLogger
)

func init() {
	rawJSON := []byte(`{
   "level": "info",
   "encoding": "console",
   "outputPaths": ["stdout"],
   "errorOutputPaths": ["stderr"],
   "encoderConfig": {
     "messageKey": "message",
     "levelKey": "level",
     "levelEncoder": "lowercase",
     "callerKey":"C"
   }
 }`)
	var cfg zap.Config
	var err error
	if err = json.Unmarshal(rawJSON, &cfg); err != nil {
		panic(err)
	}
	cfg.EncoderConfig.CallerKey = "C"
	cfg.EncoderConfig.TimeKey = "T"
	cfg.EncoderConfig.LineEnding = zapcore.DefaultLineEnding
	cfg.EncoderConfig.EncodeDuration = zapcore.StringDurationEncoder
	cfg.EncoderConfig.EncodeCaller = zapcore.FullCallerEncoder
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapLogger, err := cfg.Build(zap.AddCaller())
	if err != nil {
		panic(err)
	}
	defer zapLogger.Sync()
	logger.logger = zapLogger
	go func() {
		if err := http.ListenAndServe(":9999", nil); err != nil {
			panic(err)
		}
	}()

}
