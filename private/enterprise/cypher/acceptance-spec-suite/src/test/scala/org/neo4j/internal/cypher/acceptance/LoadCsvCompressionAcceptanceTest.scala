/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.util.zip.{DeflaterOutputStream, GZIPOutputStream}

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.server.handler.{AbstractHandler, ContextHandler, ContextHandlerCollection}
import org.eclipse.jetty.server.{Handler, Request, Server, ServerConnector}
import org.neo4j.cypher.{ExecutionEngineFunSuite, LoadExternalResourceException}
import org.scalatest.BeforeAndAfterAll

class LoadCsvCompressionAcceptanceTest extends ExecutionEngineFunSuite with BeforeAndAfterAll {
  private val CSV =
    """a1,b1,c1,d1
      |a2,b2,c2,d2""".stripMargin

  private val server = new TestServer

  override protected def afterAll() = {
    server.stop()
  }

  override protected def beforeAll() = {
    server.start()
  }

  override protected def beforeEach() = {
    super.beforeEach()
    server.restartIfNotRunning()
  }

  private def executeFlakySafe(q: String) = {
    try {
      execute(q)
    } catch {
      case _: LoadExternalResourceException =>
        System.out.println("Failed due to LoadExternalResourceException, will restart server and try this query again: '" + q + "'")
        server.restartIfNotRunning()
        execute(q)
    }
  }

  test("should handle uncompressed csv over http") {
    val result = executeFlakySafe(s"LOAD CSV FROM 'http://${server.host}:${server.port}/csv' AS lines RETURN lines")

    result.toList should equal(List(
      Map("lines" -> Seq("a1", "b1", "c1", "d1")),
      Map("lines" -> Seq("a2", "b2", "c2", "d2"))
      ))
  }

  test("should handle gzipped csv over http") {
    val result = executeFlakySafe(s"LOAD CSV FROM 'http://${server.host}:${server.port}/gzip' AS lines RETURN lines")

    result.toList should equal(List(
      Map("lines" -> Seq("a1", "b1", "c1", "d1")),
      Map("lines" -> Seq("a2", "b2", "c2", "d2"))
    ))
  }

  test("should handle deflated csv over http") {
    val result = executeFlakySafe(s"LOAD CSV FROM 'http://${server.host}:${server.port}/deflate' AS lines RETURN lines")

    result.toList should equal(List(
      Map("lines" -> Seq("a1", "b1", "c1", "d1")),
      Map("lines" -> Seq("a2", "b2", "c2", "d2"))
    ))
  }

   /*
    * Simple server that handles csv requests in plain text, gzip and deflate
    */
  private class TestServer {

     //assign the correct port when server has started.
     private var _port = -1
     private val _host = "127.0.0.1"
     //let jetty pick a random available port for us
     private val server: Server = new Server(new InetSocketAddress(_host, 0))
     private val handlers = new ContextHandlerCollection()
     addHandler("/csv", new CsvHandler)
     addHandler("/gzip", new GzipCsvHandler)
     addHandler("/deflate", new DeflateCsvHandler)
     server.setHandler(handlers)

     def start() = {
       server.start()
       //find the port that we're using.
       _port = server.getConnectors()(0).asInstanceOf[ServerConnector].getLocalPort
       assert(_port > 0)
       if (!server.isRunning) throw new IllegalStateException("Started server is not running. Wtf? State: " + server.getState)
     }

     def stop() = server.stop()

     def port = _port

     def host = _host

     def restartIfNotRunning() = {
       if (!server.isRunning) {
         stop()
         start()
       }
     }

     private def addHandler(path: String, handler: Handler): Unit = {
      val contextHandler = new ContextHandler()
      contextHandler.setContextPath(path)
      contextHandler.setHandler(handler)
      handlers.addHandler(contextHandler)
    }
  }

  /*
   * Returns csv in plain text
   */
  private class CsvHandler extends AbstractHandler {

    override def handle(s: String, request: Request, httpServletRequest: HttpServletRequest,
                        httpServletResponse: HttpServletResponse): Unit = {
      httpServletResponse.setContentType("text/csv")
      httpServletResponse.setStatus(HttpServletResponse.SC_OK)
      httpServletResponse.getWriter.print(CSV)
      request.setHandled(true)
    }
  }

  /*
   * Returns csv compressed with gzip
   */
  private class GzipCsvHandler extends AbstractHandler {

    override def handle(s: String, request: Request, httpServletRequest: HttpServletRequest,
                        httpServletResponse: HttpServletResponse): Unit = {
      httpServletResponse.setContentType("text/csv")
      httpServletResponse.setStatus(HttpServletResponse.SC_OK)
      httpServletResponse.setHeader("content-encoding", "gzip")
      //write compressed data to a byte array
      val stream = new ByteArrayOutputStream(CSV.length)
      val gzipStream = new GZIPOutputStream(stream)
      gzipStream.write(CSV.getBytes)
      gzipStream.close()
      val compressed = stream.toByteArray
      stream.close()

      //respond with the compressed data
      httpServletResponse.getOutputStream.write(compressed)
      request.setHandled(true)
    }
  }

  /*
   * Returns csv compressed with deflate
   */
  private class DeflateCsvHandler extends AbstractHandler {

    override def handle(s: String, request: Request, httpServletRequest: HttpServletRequest,
                        httpServletResponse: HttpServletResponse): Unit = {
      httpServletResponse.setContentType("text/csv")
      httpServletResponse.setStatus(HttpServletResponse.SC_OK)
      httpServletResponse.setHeader("content-encoding", "deflate")

      //write deflated data to byte array
      val stream = new ByteArrayOutputStream(CSV.length)
      val deflateStream = new DeflaterOutputStream(stream)
      deflateStream.write(CSV.getBytes)
      deflateStream.close()
      val compressed = stream.toByteArray
      stream.close()

      //respond with the deflated data
      httpServletResponse.getOutputStream.write(compressed)
      request.setHandled(true)
    }
  }
}
