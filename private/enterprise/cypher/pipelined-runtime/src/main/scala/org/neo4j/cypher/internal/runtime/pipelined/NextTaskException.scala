/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

case class NextTaskException(pipeline: ExecutablePipeline, cause: Throwable) extends Exception(cause)
case class SchedulingInputException[T](input: T, cause: Throwable) extends Exception(cause)
