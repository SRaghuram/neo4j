/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness;

import com.neo4j.harness.internal.CommercialInProcessServerBuilder;

import java.io.File;

import org.neo4j.harness.TestServerBuilder;

/**
 * Factories for creating {@link TestServerBuilder} instances.
 */
public final class CommercialTestServerBuilders
{
    /**
     * Create a builder capable of starting an in-process Neo4j instance. This builder will use the standard java temp
     * directory (configured via the 'java.io.tmpdir' system property) as the location for the temporary Neo4j directory.
     */
    public static TestServerBuilder newInProcessBuilder()
    {
        return new CommercialInProcessServerBuilder();
    }

    /**
     * Create a builder capable of starting an in-process Neo4j instance, running in a subdirectory of the specified directory.
     */
    public static TestServerBuilder newInProcessBuilder( File workingDirectory )
    {
        return new CommercialInProcessServerBuilder( workingDirectory );
    }

    private CommercialTestServerBuilders(){}
}
