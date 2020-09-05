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

package sec

import java.util.regex.Pattern
import sec.api.Endpoint

object helpers {

  object text {

    final private val basePattern: Pattern = Pattern.compile("([A-Z]+)([A-Z][a-z])")
    final private val swapPattern: Pattern = Pattern.compile("([a-z\\d])([A-Z])")
    final val snakeCaseTransformation: String => String = s => {
      val partial = basePattern.matcher(s).replaceAll("$1_$2")
      swapPattern.matcher(partial).replaceAll("$1_$2").toLowerCase
    }
  }

  object endpoint {

    def endpointFrom(envAddrName: String, envPortName: String, fallbackAddr: String, fallbackPort: Int): Endpoint = {
      val address = sys.env.getOrElse(envAddrName, fallbackAddr)
      val port    = sys.env.get(envPortName).flatMap(_.toIntOption).getOrElse(fallbackPort)
      Endpoint(address, port)
    }

  }

}