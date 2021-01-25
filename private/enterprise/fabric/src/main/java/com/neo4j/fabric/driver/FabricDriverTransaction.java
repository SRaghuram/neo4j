/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import reactor.core.publisher.Mono;

import org.neo4j.fabric.bookmark.RemoteBookmark;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.values.virtual.MapValue;

public interface FabricDriverTransaction
{
    Mono<RemoteBookmark> commit();

    Mono<Void> rollback();

    StatementResult run( String query, MapValue params );
}
