/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream;

import reactor.core.publisher.Mono;

import org.neo4j.graphdb.QueryExecutionType;

public interface FabricExecutionStatementResult extends StatementResult
{
    Mono<QueryExecutionType> queryExecutionType();
}
