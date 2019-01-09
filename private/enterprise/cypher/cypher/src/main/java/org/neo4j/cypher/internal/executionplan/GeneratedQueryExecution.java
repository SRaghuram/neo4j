/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.executionplan;

import org.neo4j.cypher.internal.runtime.ExecutionMode;
import org.neo4j.cypher.result.QueryResult;

public interface GeneratedQueryExecution
{
    <E extends Exception> void accept( QueryResult.QueryResultVisitor<E> visitor ) throws E;

    ExecutionMode executionMode();

    String[] fieldNames();
}
