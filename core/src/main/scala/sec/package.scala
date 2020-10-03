/*
 * Copyright 2020 Alex Henning Johannessen
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
 */

import cats.{ApplicativeError, MonadError}

package object sec {

  import utilities._

//======================================================================================================================

  private[sec] type ErrorM[F[_]] = MonadError[F, Throwable]
  private[sec] type ErrorA[F[_]] = ApplicativeError[F, Throwable]
  private[sec] type Attempt[T]   = Either[String, T]
  private[sec] type ErrorOr[T]   = Either[Throwable, T]

//======================================================================================================================

  implicit private[sec] def syntaxForBoolean(b: Boolean): BooleanOps          = new BooleanOps(b)
  implicit private[sec] def syntaxForAttempt[A](a: Attempt[A]): AttemptOps[A] = new AttemptOps[A](a)

//======================================================================================================================

}
