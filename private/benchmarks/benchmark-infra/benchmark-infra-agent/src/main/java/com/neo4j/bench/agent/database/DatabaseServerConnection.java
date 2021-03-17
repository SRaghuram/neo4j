/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent.database;

import com.neo4j.bench.common.process.Pid;

import java.net.URI;

public interface DatabaseServerConnection
{
    URI boltUri();

    Pid pid();
}
