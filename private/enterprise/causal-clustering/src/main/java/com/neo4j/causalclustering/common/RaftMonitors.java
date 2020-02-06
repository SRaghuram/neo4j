/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import org.neo4j.collection.Dependencies;
import org.neo4j.kernel.database.Database;
import org.neo4j.monitoring.Monitors;

/**
 * Monitors that are used by clustering components created before the {@link Database}.
 * Database creates monitors only when it is started and so some clustering components
 * that are created before the database can't retrieve database monitors.
 * <p>
 * This is basically a marker type so that metrics can dependency-resolve the correct {@link Monitors} instance.
 */
public class RaftMonitors extends Monitors
{
    private RaftMonitors( Monitors parent )
    {
        super( parent );
    }

    public static RaftMonitors create( Monitors globalMonitors, Dependencies clusterDependencies )
    {
        var monitors = new RaftMonitors( globalMonitors );
        // add monitors to the dependency resolver so that metrics extension can resolve them
        clusterDependencies.satisfyDependency( monitors );
        return monitors;
    }
}
