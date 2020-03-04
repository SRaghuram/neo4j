/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import reactor.core.publisher.Mono;

/**
 * A transaction executing against a single database.
 * Fabric transactions are composite transactions consisting of transactions of this type.
 */
public interface SingleDbTransaction
{
    Mono<Void> commit();

    Mono<Void> rollback();

    Location getLocation();
}
