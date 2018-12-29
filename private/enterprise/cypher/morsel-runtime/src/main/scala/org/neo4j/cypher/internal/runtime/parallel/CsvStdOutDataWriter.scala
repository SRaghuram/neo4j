/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

/**
  * DataPointWriter which accepts DataPoints, formats as CSV, and prints to std out.
  */
class CsvStdOutDataWriter extends CsvDataWriter {

  private val sb = new StringBuilder(header)

  def flush(): Unit = {
    val result = sb.result()
    sb.clear()
    print(result)
  }

  override def writeRow(row: String): Unit = sb ++= row
}
