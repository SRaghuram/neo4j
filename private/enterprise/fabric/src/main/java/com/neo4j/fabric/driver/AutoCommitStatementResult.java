/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import reactor.core.publisher.Mono;

import org.neo4j.fabric.bookmark.RemoteBookmark;
import org.neo4j.fabric.stream.StatementResult;

public interface AutoCommitStatementResult extends StatementResult
{
    Mono<RemoteBookmark> getBookmark();
}
