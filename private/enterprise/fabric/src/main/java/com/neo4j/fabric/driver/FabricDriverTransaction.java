/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.stream.StatementResult;
import reactor.core.publisher.Mono;

import org.neo4j.values.virtual.MapValue;

public interface FabricDriverTransaction
{
    Mono<String> commit();

    Mono<Void> rollback();

    StatementResult run( String query, MapValue params );
}
