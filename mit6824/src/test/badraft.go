package main

import (
	"encoding/json"
	"go.uber.org/zap"
)

func main() {
	rawJSON := []byte(`{
   "level": "warn",
   "encoding": "json",
   "outputPaths": ["stdout"],
   "errorOutputPaths": ["stderr"],
   "encoderConfig": {
     "messageKey": "message",
     "levelKey": "level",
     "levelEncoder": "lowercase"
   }
 }`)
	var cfg zap.Config
	if err := json.Unmarshal(rawJSON, &cfg); err != nil {
		panic(err)
	}
	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()
	logger.Info("Hi, custom logger!")
	logger.Warn("Custom logger is warning you!")
	logger.Error("Let's do error instead.")
	logger.Debug("fuck")
}
