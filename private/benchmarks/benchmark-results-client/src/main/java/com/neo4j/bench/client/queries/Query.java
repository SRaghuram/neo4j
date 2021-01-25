/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries;

import java.util.Optional;

import org.neo4j.driver.Driver;

public interface Query<RESULT>
{
    RESULT execute( Driver driver );

    Optional<String> nonFatalError();
}
