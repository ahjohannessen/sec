/*
 * Copyright 2020 Scala Event Sourcing Client for KurrentDB
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
import scala.concurrent.duration.*
import cats.data.NonEmptySet
import cats.syntax.all.*
import com.comcast.ip4s.*
import com.typesafe.config.*
import org.typelevel.log4cats.noop.NoOpLogger
import sec.tsc.config.*
import sec.api.*
import sec.api.cluster.*
import sec.api.pool.{Limit, PoolConfig}

class ConfigSuite extends SecSuite:

  group("mkSingleNodeBuilder") {

    test("no config") {

      val builder = mkSingleNodeBuilder[ErrorOr](Options.default, ConfigFactory.parseString(""))

      assertEquals(builder.authority, None)
      assertEquals(builder.endpoint, Endpoint("127.0.0.1", 2113))

    }

    test("config") {

      val cfg =
        ConfigFactory.parseString("""
          | sec.authority = "example.org"
          | sec.address   = "10.0.0.2"
          | sec.port      = 12113
          |""".stripMargin)

      val builder = mkSingleNodeBuilder[ErrorOr](Options.default, cfg)

      assertEquals(builder.authority, Some("example.org"))
      assertEquals(builder.endpoint, Endpoint("10.0.0.2", 12113))

    }

    test("partial config") {

      val cfg1 =
        ConfigFactory.parseString("""
          | sec.address = "10.0.0.3"
          |""".stripMargin)

      val cfg2 =
        ConfigFactory
          .parseString(s"""
          | sec.authority = $${?NOT_DEFINED}
          | sec.address   = "10.0.0.3"
          |""".stripMargin)
          .resolve()

      val builder1 = mkSingleNodeBuilder[ErrorOr](Options.default, cfg1)
      assertEquals(builder1.authority, None)
      assertEquals(builder1.endpoint, Endpoint("10.0.0.3", 2113))

      val builder2 = mkSingleNodeBuilder[ErrorOr](Options.default, cfg2)
      assertEquals(builder2.authority, None)
      assertEquals(builder2.endpoint, Endpoint("10.0.0.3", 2113))

    }

  }

  group("mkClusterBuilder") {

    test("no config") {
      assertEquals(
        mkClusterBuilder[ErrorOr](Options.default, ClusterOptions.default, ConfigFactory.parseString("")),
        None
      )
    }

    test("config - dns") {

      val cfg1 = ConfigFactory.parseString(
        """
            | sec.authority   = "example.org"
            | sec.cluster.dns = "active.example.org"
            |""".stripMargin
      )

      val cfg2 = ConfigFactory.parseString(
        """
            | sec.cluster.dns = "example.org"
            |""".stripMargin
      )

      mkClusterBuilder[ErrorOr](Options.default, ClusterOptions.default, cfg1).fold(
        fail("Expected some ClusterBuilder")) { b =>
        assertEquals(b.authority, "example.org")
        assertEquals(b.endpoints, ClusterEndpoints.ViaDns(host"active.example.org"))
      }

      mkClusterBuilder[ErrorOr](Options.default, ClusterOptions.default, cfg2).fold(
        fail("Expected some ClusterBuilder")) { b =>
        assertEquals(b.authority, "example.org")
        assertEquals(b.endpoints, ClusterEndpoints.ViaDns(host"example.org"))
      }

    }

    test("config - seed") {

      val expectedEndpoints = ClusterEndpoints.ViaSeed(
        NonEmptySet.of(
          Endpoint("127.0.0.1", 2113),
          Endpoint("127.0.0.2", 2213),
          Endpoint("127.0.0.3", 2113)
        )
      )

      val cfg1 = ConfigFactory.parseString(
        """
            | sec.authority    = "example.org"
            | sec.cluster.seed = [ "127.0.0.1", "127.0.0.2:2213", "127.0.0.3" ]
            |""".stripMargin
      )

      mkClusterBuilder[ErrorOr](Options.default, ClusterOptions.default, cfg1)
        .fold(fail("Expected some ClusterBuilder")) { b =>
          assertEquals(b.authority, "example.org")
          assertEquals(b.endpoints, expectedEndpoints)
        }

      val cfg2 = ConfigFactory.parseString(
        """
            | sec.authority    = "example.org"
            | sec.cluster.seed = "127.0.0.1,127.0.0.2:2213, 127.0.0.3, "
            |""".stripMargin
      )

      mkClusterBuilder[ErrorOr](Options.default, ClusterOptions.default, cfg2)
        .fold(fail("Expected some ClusterBuilder")) { b =>
          assertEquals(b.authority, "example.org")
          assertEquals(b.endpoints, expectedEndpoints)
        }

    }

    test("partial config") {

      val cfg1 = ConfigFactory.parseString(
        """
            | sec.authority    = "example.org"
            | sec.cluster.seed = []
            |""".stripMargin
      )

      val cfg2 = ConfigFactory.parseString(
        """
            | sec.cluster.seed = [ "127.0.0.1", "127.0.0.2:2213", "127.0.0.3" ]
            |""".stripMargin
      )

      assertEquals(mkClusterBuilder[ErrorOr](Options.default, ClusterOptions.default, cfg1), None)
      assertEquals(mkClusterBuilder[ErrorOr](Options.default, ClusterOptions.default, cfg2), None)

    }

  }

  group("mkBuilder") {

    val noopL = NoOpLogger[ErrorOr]
    val noopR = EndpointResolver.noop[ErrorOr]

    test("no config") {

      mkBuilder[ErrorOr](ConfigFactory.parseString(""), noopL, noopR) match {
        case Left(e)       => fail(e.getMessage, e)
        case Right(either) =>
          either match {
            case Left(_)  => fail("did not expect cluster builder")
            case Right(s) =>
              assertEquals(s.authority, None)
              assertEquals(s.endpoint, Endpoint("127.0.0.1", 2113))
          }
      }

    }

    test("single node config") {

      val cfg =
        ConfigFactory.parseString("""
          | sec.authority = "example.org"
          | sec.address   = "10.0.0.2"
          |""".stripMargin)

      mkBuilder[ErrorOr](cfg, noopL, noopR) match {
        case Left(e)       => fail(e.getMessage, e)
        case Right(either) =>
          either match {
            case Left(_)  => fail("did not expect cluster builder")
            case Right(s) =>
              assertEquals(s.authority, Some("example.org"))
              assertEquals(s.endpoint, Endpoint("10.0.0.2", 2113))
          }
      }

    }

    test("cluster config") {

      val cfg1 = ConfigFactory.parseString(
        """
            | sec.authority   = "example.org"
            | sec.cluster.dns = "rendezvous.example.org"
            | sec.cluster.seed = [ "127.0.0.1:2113", "127.0.0.2:2113", "127.0.0.3:2113" ]
            |""".stripMargin
      )

      mkBuilder[ErrorOr](cfg1, noopL, noopR) match {
        case Left(e)       => fail(e.getMessage, e)
        case Right(either) =>
          either match {
            case Right(_) => fail("did not expect single node builder")
            case Left(c)  =>
              assertEquals(c.authority, "example.org")
              assertEquals(c.endpoints, ClusterEndpoints.ViaDns(host"rendezvous.example.org"))
          }
      }

      val cfg2 = ConfigFactory.parseString(
        """
            | sec.authority    = "example.org"
            | sec.cluster.dns  = ""
            | sec.cluster.seed = [ "127.0.0.1:2113", "127.0.0.2:2113", "127.0.0.3:2113" ]
            |""".stripMargin
      )

      mkBuilder[ErrorOr](cfg2, noopL, noopR) match {
        case Left(e)       => fail(e.getMessage, e)
        case Right(either) =>
          either match {
            case Right(_) => fail("did not expect single node builder")
            case Left(c)  =>
              assertEquals(c.authority, "example.org")
              assertEquals(
                c.endpoints,
                ClusterEndpoints.ViaSeed(
                  NonEmptySet.of(
                    Endpoint("127.0.0.1", 2113),
                    Endpoint("127.0.0.2", 2113),
                    Endpoint("127.0.0.3", 2113)
                  )
                )
              )
          }
      }

    }

  }

  group("mkOptions") {

    test("no config") {
      assertEquals(mkOptions[ErrorOr](ConfigFactory.parseString("")), Right(Options.default))
    }

    test("config") {

      val cfg = ConfigFactory.parseString(
        """
          | sec {
          | 
          |   connection-name          = "config-client"
          |   certificate-path         = "path/to/certificate"
          |   username                 = "mr"
          |   password                 = "mr"
          |   channel-shutdown-await   = 20s
          |   prefetch-n-messages      = 1
              max-inbound-message-size = 16777216
          |   port                     = 2115
          | 
          |   operations {
          |   
          |     retry-enabled        = false
          |     retry-delay          = 2500ms
          |     retry-max-delay      = 5s
          |     retry-backoff-factor = 2
          |     retry-max-attempts   = 1000
          |
          |     subscription-confirmation-timeout = 30s
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
        .withPrefetchN(1)
        .withMaxInboundMessageSize(16777216)
        .withHttpPort(port"2115")
        .withOperationsRetryEnabled(false)
        .withOperationsRetryDelay(2500.millis)
        .withOperationsRetryMaxDelay(5.seconds)
        .withOperationsRetryBackoffFactor(2)
        .withOperationsRetryMaxAttempts(1000)
        .withSubscriptionConfirmationTimeout(30.seconds)

      assertEquals(mkOptions[ErrorOr](cfg), Right(expected))

    }

    test("config/certificate") {

      val cfg1 = ConfigFactory.parseString(
        """
          | sec {
          |   certificate-path = "path/to/certificate"
          |   certificate-b64  = "cTaciKkQb2IAgPPfl1OdE3ErJtHyRXNbLAcI0ISciS4="
          | }
          |""".stripMargin
      )

      assertEquals(
        mkOptions[ErrorOr](cfg1),
        Options.default.withSecureMode(new File("path/to/certificate")).asRight
      )

      val cfg2 = ConfigFactory.parseString(
        """sec.certificate-b64 = "cTaciKkQb2IAgPPfl1OdE3ErJtHyRXNbLAcI0ISciS4=""""
      )

      assertEquals(
        mkOptions[ErrorOr](cfg2),
        Options.default.withSecureMode("cTaciKkQb2IAgPPfl1OdE3ErJtHyRXNbLAcI0ISciS4=").asRight
      )

      val cfg3 = ConfigFactory.parseString(
        """
          | sec {
          |   certificate-path = ""
          |   certificate-b64 = ""
          | }
          |""".stripMargin
      )

      assertEquals(mkOptions[ErrorOr](cfg3), Options.default.withInsecureMode.asRight)

    }

  }

  group("mkPoolConfig") {

    test("no config") {
      assertEquals(mkPoolConfig[ErrorOr](ConfigFactory.parseString("")), Right(None))
    }

    test("streams-per-channel is required - no silent default") {

      val cfg = ConfigFactory.parseString(
        """
          | sec.subscription-pool {
          |   enabled = true
          | }
          |""".stripMargin
      )

      assertEquals(mkPoolConfig[ErrorOr](cfg), Right(None))

    }

    test("config - bounded") {

      val cfg1 = ConfigFactory.parseString(
        """
          | sec.subscription-pool {
          |   enabled             = true
          |   streams-per-channel = 100
          | }
          |""".stripMargin
      )

      assertEquals(mkPoolConfig[ErrorOr](cfg1), PoolConfig(100, Limit.default).map(_.some))

      val cfg2 = ConfigFactory.parseString(
        """
          | sec.subscription-pool {
          |   enabled             = true
          |   streams-per-channel = 100
          |   limit               = bounded
          |   max-channels        = 5
          | }
          |""".stripMargin
      )

      assertEquals(mkPoolConfig[ErrorOr](cfg2), PoolConfig(100, Limit.Bounded(5)).map(_.some))

    }

    test("config - unbounded") {

      val cfg2 = ConfigFactory.parseString(
        """
          | sec.subscription-pool {
          |   enabled             = true
          |   streams-per-channel = 100
          |   limit               = unbounded
          |   sanity-cap          = 20
          | }
          |""".stripMargin
      )

      assertEquals(mkPoolConfig[ErrorOr](cfg2), PoolConfig(100, Limit.Unbounded(20)).map(_.some))

    }

    test("disabled unless explicitly opted in via enabled = true") {

      val withoutEnabled = ConfigFactory.parseString(
        """
          | sec.subscription-pool {
          |   streams-per-channel = 100
          | }
          |""".stripMargin
      )

      val killSwitch = ConfigFactory.parseString(
        """
          | sec.subscription-pool {
          |   enabled             = false
          |   streams-per-channel = 100
          | }
          |""".stripMargin
      )

      assertEquals(mkPoolConfig[ErrorOr](withoutEnabled), Right(None))
      assertEquals(mkPoolConfig[ErrorOr](killSwitch), Right(None))

    }

    test("strict parsing - malformed values raise instead of failing open") {

      def poolCfg(body: String) =
        ConfigFactory.parseString(s"sec.subscription-pool { $body }")

      // a mangled switch must not be silently treated as absent, in either direction
      assert(mkPoolConfig[ErrorOr](poolCfg("enabled = flase, streams-per-channel = 100")).isLeft)
      assert(mkPoolConfig[ErrorOr](poolCfg("enabled = ture, streams-per-channel = 100")).isLeft)
      // a limit typo must not silently fall back to bounded
      assert(mkPoolConfig[ErrorOr](poolCfg("streams-per-channel = 100, limit = unbouned")).isLeft)
      // naming a limit kind without its size is half a config
      assert(mkPoolConfig[ErrorOr](poolCfg("streams-per-channel = 100, limit = bounded")).isLeft)
      assert(mkPoolConfig[ErrorOr](poolCfg("streams-per-channel = 100, limit = unbounded")).isLeft)
      // non-numeric values raise
      assert(mkPoolConfig[ErrorOr](poolCfg("streams-per-channel = many")).isLeft)
      // PoolConfig validation surfaces through the parser
      assert(mkPoolConfig[ErrorOr](poolCfg("streams-per-channel = 0")).isLeft)
      assert(mkPoolConfig[ErrorOr](poolCfg("streams-per-channel = 100, limit = bounded, max-channels = 0")).isLeft)
      // a size key without its kind - or with the wrong kind - would otherwise be silently ignored
      assert(mkPoolConfig[ErrorOr](poolCfg("streams-per-channel = 100, max-channels = 5")).isLeft)
      assert(mkPoolConfig[ErrorOr](poolCfg("streams-per-channel = 100, sanity-cap = 5")).isLeft)
      assert(mkPoolConfig[ErrorOr](poolCfg("streams-per-channel = 100, limit = bounded, sanity-cap = 5")).isLeft)
      assert(
        mkPoolConfig[ErrorOr](
          poolCfg("streams-per-channel = 100, limit = unbounded, max-channels = 5, sanity-cap = 5")).isLeft)

    }

  }

  group("mkBuilder with subscription pool") {

    val noopL = NoOpLogger[ErrorOr]
    val noopR = EndpointResolver.noop[ErrorOr]

    val poolCfg =
      """
        | sec.subscription-pool {
        |   enabled             = true
        |   streams-per-channel = 100
        |   limit               = bounded
        |   max-channels        = 5
        | }
        |""".stripMargin

    test("single node") {

      mkBuilder[ErrorOr](ConfigFactory.parseString(poolCfg), noopL, noopR) match {
        case Left(e)               => fail(e.getMessage, e)
        case Right(Left(_))        => fail("did not expect cluster builder")
        case Right(Right(builder)) =>
          assertEquals(builder.subscriptionPool, PoolConfig(100, Limit.Bounded(5)).toOption)
      }

    }

    test("cluster") {

      val cfg = ConfigFactory.parseString(
        s"""
           | sec.authority   = "example.org"
           | sec.cluster.dns = "active.example.org"
           |$poolCfg
           |""".stripMargin
      )

      mkBuilder[ErrorOr](cfg, noopL, noopR) match {
        case Left(e)              => fail(e.getMessage, e)
        case Right(Right(_))      => fail("did not expect single node builder")
        case Right(Left(builder)) =>
          assertEquals(builder.subscriptionPool, PoolConfig(100, Limit.Bounded(5)).toOption)
      }

    }

  }

  group("mkClusterOptions") {

    test("no config") {
      assertEquals(mkClusterOptions[ErrorOr](ConfigFactory.parseString("")), Right(ClusterOptions.default))
    }

    test("config") {

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

      assertEquals(mkClusterOptions[ErrorOr](cfg), Right(expected))

    }

  }
