/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.ssl

import java.util.concurrent.TimeUnit

import com.neo4j.ssl.SslContextFactory.SslParameters
import com.neo4j.ssl.SslNegotiationTest.TLSv1_0
import com.neo4j.ssl.SslNegotiationTest.TLSv1_1
import com.neo4j.ssl.SslNegotiationTest.TLSv1_2
import com.neo4j.ssl.SslNegotiationTest.TLSv1_3
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory
import org.junit.runner.Description
import org.junit.runner.RunWith
import org.junit.runners.model.Statement
import org.neo4j.configuration.ssl.SslPolicyScope
import org.neo4j.ssl.SslResourceBuilder.selfSignedKeyId
import org.neo4j.test.rule.TestDirectory
import org.scalacheck.Gen
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.util.control.NonFatal

@RunWith(classOf[JUnitRunner])
class SslNegotiationTest extends WordSpecLike with GeneratorDrivenPropertyChecks with Matchers {
  "Ssl negotiation" should {
    "not connect if protocols/ciphers in common are incompatible or none in common" in {
      assume(enabledProtocols1_2.nonEmpty)
      assume(enabledCiphers1_2.nonEmpty)
      withTestDirectory { testDir =>
        forAll ((setupGen1_2, "client"), (setupGen1_2, "server"), sizeRange(5), minSuccessful(20), maxDiscardedFactor(500d))
        { (clientSetup: Setup, serverSetup: Setup) =>
          val protocolsInCommon = clientSetup.protocols intersect serverSetup.protocols
          val ciphersInCommon = clientSetup.ciphers intersect serverSetup.ciphers

          whenever(!hasCompatibleProtocols(protocolsInCommon, ciphersInCommon)) {
            withClient(testDir, clientSetup, serverSetup)(assertCannotConnect(protocolsInCommon, ciphersInCommon))
          }
        }
      }
    }
    "connect if protocols (TLSv1 - TLSv1.2) and ciphers in common" in {
      assume(enabledProtocols1_2.nonEmpty)
      assume(enabledCiphers1_2.nonEmpty)
      withTestDirectory { testDir =>
        forAll ((setupGen1_2, "client"), (setupGen1_2, "server"), sizeRange(5), minSuccessful(100), maxDiscardedFactor(50d))
        { (clientSetup: Setup, serverSetup: Setup) =>
          val protocolsInCommon = clientSetup.protocols intersect serverSetup.protocols
          val ciphersInCommon = clientSetup.ciphers intersect serverSetup.ciphers

          whenever(hasCompatibleProtocols(protocolsInCommon, ciphersInCommon)) {
            withClient(testDir, clientSetup, serverSetup)(assertCanConnect(protocolsInCommon, ciphersInCommon))
          }
        }
      }
    }
    "connect if protocol TLSv1.3 and ciphers in common" in {
      assume(enabledProtocols1_3.nonEmpty)
      assume(enabledCiphers1_3.nonEmpty)
      withTestDirectory { testDir =>
        forAll ((setupGen1_3, "client"), (setupGen1_3, "server"), sizeRange(5), minSuccessful(10), maxDiscardedFactor(50d))
        { (clientSetup: Setup, serverSetup: Setup) =>
          val protocolsInCommon = clientSetup.protocols intersect serverSetup.protocols
          val ciphersInCommon = clientSetup.ciphers intersect serverSetup.ciphers

          whenever(hasCompatibleProtocols(protocolsInCommon, ciphersInCommon)) {
            withClient(testDir, clientSetup, serverSetup)(assertCanConnect(protocolsInCommon, ciphersInCommon))
          }
        }
      }
    }
  }

  private val testDirRule = TestDirectory.testDirectory(classOf[SslNegotiationTest])

  def withTestDirectory(testCode: TestDirectory => Any): Unit = {
    testDirRule.apply(
      new Statement() {
        override def evaluate(): Unit = testCode(testDirRule)
      },
      Description.createSuiteDescription("Test directory rule wrapper")
    ).evaluate()
  }

  def withClient(testDir: TestDirectory, clientSetup: Setup, serverSetup: Setup)(test: SecureClient => Unit): Unit = {
    val sslServerResource = selfSignedKeyId(0).trustKeyId(1).install(testDir.directory("server"))
    val sslClientResource = selfSignedKeyId(1).trustKeyId(0).install(testDir.directory("client"))

    var server: SecureServer = null
    var client: SecureClient = null

    try {
      server = new SecureServer(SslContextFactory.makeSslPolicy(sslServerResource, toSslParameters(serverSetup), SslPolicyScope.TESTING))
      server.start()
      client = new SecureClient(SslContextFactory.makeSslPolicy(sslClientResource, toSslParameters(clientSetup), SslPolicyScope.TESTING))
      client.connect(server.port)

      test(client)
    }
    finally {
      if (client != null) {

        client.disconnect()
      }
      if (server != null) {
        server.stop()
      }
    }
  }

  private def toSslParameters(setup: Setup) = {
    SslParameters.protocols(setup.protocols: _*).ciphers(setup.ciphers: _*)
  }

  private val isRecentSha = """.*_SHA\d{3,4}""".r
  private def hasCompatibleProtocols(protocols: List[String], ciphers: List[String]): Boolean = {
    val permutations = for {
      protocol <- protocols
      cipher <- ciphers
    } yield (protocol,cipher)

    // SHA256 is compatible only with TLS1.2 or later
    def areCompatible(protocolCipher: (String, String)) = protocolCipher match {
      case (`TLSv1_0`, isRecentSha()) => false
      case (`TLSv1_1`, isRecentSha()) => false
      case (_, _) => true
    }

    permutations.exists(areCompatible)
  }

  case class Setup(ciphers: List[String], protocols: List[String])

  private val nettyCipherSuiteWhitelist = {
    val clazz = Class.forName("io.netty.handler.ssl.SslUtils")
    val field = clazz.getDeclaredField("DEFAULT_CIPHER_SUITES")
    field.setAccessible(true)
    val suites = field.get(null).asInstanceOf[Array[String]]
    field.setAccessible(false)
    suites
  }

  // This will fail if/when a version of TLS post 1.3 is supported
  // TLSv1.3 uses a completely different set of ciphers from 1.2 or earlier. They can be handily distinguished by the lack of the string _WITH_.
  private val (enabledCiphers1_2, enabledCiphers1_3, enabledProtocols1_2, enabledProtocols1_3) = {
    val factory = SSLSocketFactory.getDefault.asInstanceOf[SSLSocketFactory]
    val socket = factory.createSocket().asInstanceOf[SSLSocket]
    try {
      val ciphers = socket
        .getEnabledCipherSuites
        .intersect(nettyCipherSuiteWhitelist)
        .filter(!_.contains("ECDSA")) // Elliptic curve is fussy
        .filter(!_.contains("ECDHE"))
        .groupBy(cipher => {
          if (cipher contains "_WITH_") {
            TLSv1_2
          } else {
            TLSv1_3
          }
        })
      val protocols = socket
        .getEnabledProtocols
        .sorted
      // return a 4-tuple that gets deconstructed
      (
        ciphers.getOrElse(TLSv1_2, Array()),
        ciphers.getOrElse(TLSv1_3, Array()),
        protocols.filter( _ != TLSv1_3),
        protocols.filter(_ == TLSv1_3)
      )
    }
    finally {
      if (socket != null) {
        socket.close()
      }
    }
  }

  private lazy val setupGen1_2 = for {
    ciphers <- Gen.nonEmptyContainerOf[Set, String](Gen.oneOf(enabledCiphers1_2))
    firstProtocol <- Gen.chooseNum(0, enabledProtocols1_2.length - 1) // chooseNum is inclusive
    lastProtocol <- Gen.chooseNum(firstProtocol, enabledProtocols1_2.length)
  } yield Setup(ciphers.toList, enabledProtocols1_2.toList.slice(firstProtocol, lastProtocol + 1)) // slice excludes upper

  private lazy val setupGen1_3 = for {
    ciphers <- Gen.nonEmptyContainerOf[Set, String](Gen.oneOf(enabledCiphers1_3))
    protocol <- Gen.nonEmptyContainerOf[Set, String](Gen.oneOf(enabledProtocols1_3))
  } yield Setup(ciphers.toList, protocol.toList)

  private def assertCanConnect(protocolsInCommon: List[String], ciphersInCommon: List[String])(client: SecureClient) = {
    client.sslHandshakeFuture().get(1, TimeUnit.MINUTES) shouldBe 'active
    protocolsInCommon should contain(client.protocol())
    ciphersInCommon.map(_.substring(4)) should contain(client.ciphers().substring(4))
  }

  private def assertCannotConnect(protocolsInCommon: List[String], ciphersInCommon: List[String])(client: SecureClient) = {
    try {
      client.sslHandshakeFuture().get(1, TimeUnit.MINUTES).isActive
      fail("Should not have connected with no protocols or no ciphers in common")
    }
    catch {
      case NonFatal(_) => // expected
    }
  }
}

object SslNegotiationTest {
  val TLSv1_0 = "TLSv1"
  val TLSv1_1 = "TLSv1.1"
  val TLSv1_2 = "TLSv1.2"
  val TLSv1_3 = "TLSv1.3"
}
