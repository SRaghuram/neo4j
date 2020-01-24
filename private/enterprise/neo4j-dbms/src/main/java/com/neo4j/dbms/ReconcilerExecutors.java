/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Objects;
import java.util.concurrent.Executor;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class ReconcilerExecutors
{
    private final Executor standardExecutor;
    private final Executor priorityExecutor;

    ReconcilerExecutors( JobScheduler scheduler, Config config )
    {
        int parallelism = config.get( GraphDatabaseSettings.reconciler_maximum_parallelism );
        scheduler.setParallelism( Group.DATABASE_RECONCILER , parallelism );
        this.standardExecutor = scheduler.executor( Group.DATABASE_RECONCILER );
        // parallelism of the priority executor is unbounded as this executor is reserved for jobs, triggered by administrators.
        this.priorityExecutor = scheduler.executor( Group.DATABASE_RECONCILER_PRIORITY );
    }

    public Executor executor( ReconcilerRequest request, String databaseName )
    {
        var isSystem = Objects.equals( databaseName, SYSTEM_DATABASE_NAME );
        var isHighPriority = request.isPriorityRequestForDatabase( databaseName );

        return (isSystem || isHighPriority) ? priorityExecutor : standardExecutor;
    }
}
