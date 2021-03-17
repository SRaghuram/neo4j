/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent.database;

import com.neo4j.bench.common.util.Jvm;

import java.nio.file.Path;
import java.util.concurrent.TimeoutException;

public interface DatabaseServerWrapper
{
    static DatabaseServerWrapper neo4j( Path neo4jDir )
    {
        return new Neo4jServerWrapper( neo4jDir );
    }

    DatabaseServerConnection start( Jvm jvm,
                                    Path neo4jConfigFile,
                                    ProcessBuilder.Redirect outputRedirect,
                                    ProcessBuilder.Redirect errorRedirect );

    void copyLogsTo( Path destinationFolder );

    void stop() throws TimeoutException;
}
