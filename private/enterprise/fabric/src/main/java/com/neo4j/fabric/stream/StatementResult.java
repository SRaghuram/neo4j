/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream;

import com.neo4j.fabric.stream.summary.Summary;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface StatementResult
{
    Flux<String> columns();

    Flux<Record> records();

    Mono<Summary> summary();
}
