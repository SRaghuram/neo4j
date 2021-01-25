/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness;

import com.neo4j.harness.internal.EnterpriseInProcessNeo4jBuilder;

import java.io.File;
import java.nio.file.Path;

import org.neo4j.harness.Neo4jBuilder;

/**
 * Factories for creating {@link Neo4jBuilder} instances.
 */
public final class EnterpriseNeo4jBuilders
{
    /**
     * Create a builder capable of starting an in-process Neo4j instance. This builder will use the standard java temp
     * directory (configured via the 'java.io.tmpdir' system property) as the location for the temporary Neo4j directory.
     */
    public static Neo4jBuilder newInProcessBuilder()
    {
        return new EnterpriseInProcessNeo4jBuilder();
    }

    /**
     * Create a builder capable of starting an in-process Neo4j instance, running in a subdirectory of the specified directory.
     */
    @Deprecated( forRemoval = true )
    public static Neo4jBuilder newInProcessBuilder( File workingDirectory )
    {
        return newInProcessBuilder( workingDirectory.toPath() );
    }

    /**
     * Create a builder capable of starting an in-process Neo4j instance, running in a subdirectory of the specified directory.
     */
    public static Neo4jBuilder newInProcessBuilder( Path workingDirectory )
    {
        return new EnterpriseInProcessNeo4jBuilder( workingDirectory );
    }

    private EnterpriseNeo4jBuilders(){}
}
