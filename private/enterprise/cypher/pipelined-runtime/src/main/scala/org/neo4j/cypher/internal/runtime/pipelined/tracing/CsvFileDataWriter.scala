/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.tracing

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

/**
 * DataPointWriter which accepts DataPoints, formats as CSV, and prints to file.
 */
class CsvFileDataWriter(file: File) extends CsvDataWriter {

  private val out = new BufferedWriter(new FileWriter(file))
  writeRow(header)

  override def flush(): Unit = out.flush()

  override def close(): Unit = out.close()

  override def writeRow(row: String): Unit =
    out.write(row)
}
