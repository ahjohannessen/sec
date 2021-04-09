/*
 * Copyright 2020 Scala EventStoreDB Client
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
package tsc

import java.io.File
import scala.concurrent.duration._
import org.specs2.mutable.Specification
import com.typesafe.config._
import cats.data.NonEmptySet
import org.typelevel.log4cats.noop.NoOpLogger
import sec.tsc.config._
import sec.api._

class ConfigSpec extends Specification {

  "mkSingleNodeBuilder" >> {

    "no config" >> {

      val builder = mkSingleNodeBuilder[ErrorOr](Options.default, ConfigFactory.parseString(""))

      builder.authority should beNone
      builder.endpoint shouldEqual Endpoint("127.0.0.1", 2113)

    }

    "config" >> {

      val cfg =
        ConfigFactory.parseString("""
          | sec.single-node.authority = "example.org"
          | sec.single-node.address   = "10.0.0.2"
          | sec.single-node.port      = 12113
          |""".stripMargin)

      val builder = mkSingleNodeBuilder[ErrorOr](Options.default, cfg)

      builder.authority should beSome("example.org")
      builder.endpoint shouldEqual Endpoint("10.0.0.2", 12113)

    }

    "partial config" >> {

      val cfg =
        ConfigFactory.parseString("""
          | sec.single-node.address = "10.0.0.3"
          |""".stripMargin)

      val builder = mkSingleNodeBuilder[ErrorOr](Options.default, cfg)

      builder.authority should beNone
      builder.endpoint shouldEqual Endpoint("10.0.0.3", 2113)

    }

  }

  "mkClusterBuilder" >> {

    "no config" >> {

      "no config" >> {
        mkClusterBuilder[ErrorOr](Options.default, ClusterOptions.default, ConfigFactory.parseString("")) should beNone
      }

      "config" >> {

        val cfg = ConfigFactory.parseString(
          """
            | sec.cluster.authority = "example.org"
            | sec.cluster.seed      = [ "127.0.0.1", "127.0.0.2:2213", "127.0.0.3" ]
            |""".stripMargin
        )

        val builder = mkClusterBuilder[ErrorOr](Options.default, ClusterOptions.default, cfg)

        builder.fold(ko) { b =>
          b.authority shouldEqual "example.org"
          b.seed shouldEqual NonEmptySet.of(
            Endpoint("127.0.0.1", 2113),
            Endpoint("127.0.0.2", 2213),
            Endpoint("127.0.0.3", 2113)
          )
        }

      }

      "partial config" >> {

        val cfg1 = ConfigFactory.parseString(
          """
            | sec.cluster.authority = "example.org"
            | sec.cluster.seed      = []
            |""".stripMargin
        )

        val cfg2 = ConfigFactory.parseString(
          """
            | sec.cluster.seed = [ "127.0.0.1", "127.0.0.2:2213", "127.0.0.3" ]
            |""".stripMargin
        )

        mkClusterBuilder[ErrorOr](Options.default, ClusterOptions.default, cfg1) should beNone
        mkClusterBuilder[ErrorOr](Options.default, ClusterOptions.default, cfg2) should beNone

      }

    }

  }

  "mkBuilder" >> {

    "no config" >> {

      mkBuilder[ErrorOr](ConfigFactory.parseString(""), NoOpLogger[ErrorOr]) match {
        case Left(e) => ko(e.getMessage)
        case Right(either) =>
          either match {
            case Left(_) => ko("did not expect cluster builder")
            case Right(s) =>
              s.authority should beNone
              s.endpoint shouldEqual Endpoint("127.0.0.1", 2113)
          }
      }

    }

    "single node config" >> {

      val cfg =
        ConfigFactory.parseString("""
          | sec.single-node.authority = "example.org"
          | sec.single-node.address   = "10.0.0.2"
          |""".stripMargin)

      mkBuilder[ErrorOr](cfg, NoOpLogger[ErrorOr]) match {
        case Left(e) => ko(e.getMessage)
        case Right(either) =>
          either match {
            case Left(_) => ko("did not expect cluster builder")
            case Right(s) =>
              s.authority should beSome("example.org")
              s.endpoint shouldEqual Endpoint("10.0.0.2", 2113)
          }
      }

    }

    "cluster config" >> {

      val cfg = ConfigFactory.parseString(
        """
            | sec.cluster.authority = "example.org"
            | sec.cluster.seed      = [ "127.0.0.1:2113", "127.0.0.2:2113", "127.0.0.3:2113" ]
            |""".stripMargin
      )

      mkBuilder[ErrorOr](cfg, NoOpLogger[ErrorOr]) match {
        case Left(e) => ko(e.getMessage)
        case Right(either) =>
          either match {
            case Right(_) => ko("did not expect single node builder")
            case Left(c) =>
              c.authority shouldEqual "example.org"
              c.seed shouldEqual NonEmptySet.of(
                Endpoint("127.0.0.1", 2113),
                Endpoint("127.0.0.2", 2113),
                Endpoint("127.0.0.3", 2113)
              )
          }
      }

    }

  }

  "mkOptions" >> {

    "no config" >> {
      mkOptions[ErrorOr](ConfigFactory.parseString("")) should beRight(Options.default)
    }

    "config" >> {

      val cfg = ConfigFactory.parseString(
        """
          | sec {
          | 
          |   connection-name        = "config-client"
          |   certificate-path       = "path/to/certificate"
          |   username               = "mr"
          |   password               = "mr"
          |   channel-shutdown-await = 20s
          | 
          |   operations {
          |   
          |     retry-enabled        = false
          |     retry-delay          = 2500ms
          |     retry-max-delay      = 5s
          |     retry-backoff-factor = 2
          |     retry-max-attempts   = 1000
          |     
          |   }
          | }
          |""".stripMargin
      )

      val expected = Options.default
        .withConnectionName("config-client")
        .withSecureMode(new File("path/to/certificate"))
        .withCredentials(UserCredentials("mr", "mr").toOption)
        .withChannelShutdownAwait(20.seconds)
        .withOperationsRetryEnabled(false)
        .withOperationsRetryDelay(2500.millis)
        .withOperationsRetryMaxDelay(5.seconds)
        .withOperationsRetryBackoffFactor(2)
        .withOperationsRetryMaxAttempts(1000)

      mkOptions[ErrorOr](cfg) shouldEqual Right(expected)

    }

  }

  "mkClusterOptions" >> {

    "no config" >> {
      mkClusterOptions[ErrorOr](ConfigFactory.parseString("")) should beRight(ClusterOptions.default)
    }

    "config" >> {

      val cfg = ConfigFactory.parseString(
        """
          | sec {
          | 
          |   cluster {
          |   
          |      options {
          |
          |         node-preference        = read-only-replica
          |         max-discovery-attempts = -1
          |         retry-delay            = 120ms
          |         retry-max-delay        = 3s
          |         retry-backoff-factor   = 1.3        
          |         notification-interval  = 150ms
          |         read-timeout           = 2s
          |
          |      }
          |   }
          | }
          |""".stripMargin
      )

      val expected = ClusterOptions.default
        .withNodePreference(NodePreference.ReadOnlyReplica)
        .withMaxDiscoverAttempts(None)
        .withRetryDelay(120.millis)
        .withRetryMaxDelay(3.seconds)
        .withRetryBackoffFactor(1.3)
        .withNotificationInterval(150.millis)
        .withReadTimeout(2.seconds)

      mkClusterOptions[ErrorOr](cfg) should beRight(expected)

    }

  }

}
