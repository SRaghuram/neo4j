/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.exceptions.CypherExecutionException

class ReferenceCountingException(msg: String, cause: Throwable = null) extends CypherExecutionException(msg, cause)

