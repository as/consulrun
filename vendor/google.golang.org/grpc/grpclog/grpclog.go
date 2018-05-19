/*
 *
 * Copyright 2017 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

//
//
package grpclog // import "google.golang.org/grpc/grpclog"

import "os"

var logger = newLoggerV2()

func V(l int) bool {
	return logger.V(l)
}

func Info(args ...interface{}) {
	logger.Info(args...)
}

func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

func Infoln(args ...interface{}) {
	logger.Infoln(args...)
}

func Warning(args ...interface{}) {
	logger.Warning(args...)
}

func Warningf(format string, args ...interface{}) {
	logger.Warningf(format, args...)
}

func Warningln(args ...interface{}) {
	logger.Warningln(args...)
}

func Error(args ...interface{}) {
	logger.Error(args...)
}

func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

func Errorln(args ...interface{}) {
	logger.Errorln(args...)
}

func Fatal(args ...interface{}) {
	logger.Fatal(args...)

	os.Exit(1)
}

func Fatalf(format string, args ...interface{}) {
	logger.Fatalf(format, args...)

	os.Exit(1)
}

func Fatalln(args ...interface{}) {
	logger.Fatalln(args...)

	os.Exit(1)
}

func Print(args ...interface{}) {
	logger.Info(args...)
}

func Printf(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

func Println(args ...interface{}) {
	logger.Infoln(args...)
}
