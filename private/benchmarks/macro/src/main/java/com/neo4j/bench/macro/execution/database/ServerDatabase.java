/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;

import java.net.URI;

import org.neo4j.driver.Session;

public interface ServerDatabase extends Database
{
    URI boltUri();

    Session session();

    @Override
    void close();
}
