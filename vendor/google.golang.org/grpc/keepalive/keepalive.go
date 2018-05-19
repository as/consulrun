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

package keepalive

import (
	"time"
)

type ClientParameters struct {
	Time time.Duration // The current default value is infinity.

	Timeout time.Duration // The current default value is 20 seconds.

	PermitWithoutStream bool // false by default.
}

type ServerParameters struct {
	MaxConnectionIdle time.Duration // The current default value is infinity.

	MaxConnectionAge time.Duration // The current default value is infinity.

	MaxConnectionAgeGrace time.Duration // The current default value is infinity.

	Time time.Duration // The current default value is 2 hours.

	Timeout time.Duration // The current default value is 20 seconds.
}

type EnforcementPolicy struct {
	MinTime time.Duration // The current default value is 5 minutes.

	PermitWithoutStream bool // false by default.
}
