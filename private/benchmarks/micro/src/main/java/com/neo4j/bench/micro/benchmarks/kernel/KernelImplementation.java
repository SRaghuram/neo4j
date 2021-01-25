/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public enum KernelImplementation
{
    records
            {
                @Override
                Kernel start( GraphDatabaseAPI graphDB )
                {
                    return graphDB.getDependencyResolver().resolveDependency( Kernel.class );
                }
            };

    abstract Kernel start( GraphDatabaseAPI graphDB );
}
