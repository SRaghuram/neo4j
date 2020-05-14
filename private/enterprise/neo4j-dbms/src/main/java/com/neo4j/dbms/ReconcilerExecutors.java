/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.concurrent.Executor;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

public class ReconcilerExecutors
{
    private final Executor standardExecutor;
    private final Executor unboundExecutor;

    ReconcilerExecutors( JobScheduler scheduler, Config config )
    {
        int parallelism = config.get( GraphDatabaseSettings.reconciler_maximum_parallelism );
        scheduler.setParallelism( Group.DATABASE_RECONCILER , parallelism );
        this.standardExecutor = scheduler.executor( Group.DATABASE_RECONCILER );
        this.unboundExecutor = scheduler.executor( Group.DATABASE_RECONCILER_UNBOUND );
    }

    Executor unboundExecutor()
    {
        return unboundExecutor;
    }

    Executor executor( ReconcilerRequest request, NamedDatabaseId databaseId )
    {
        // We use the unbound executor for priority and system database reconciliation jobs
        var isSystem = databaseId.isSystemDatabase();
        var isHighPriority = request.shouldBeExecutedAsPriorityFor( databaseId.name() );

        return (isSystem || isHighPriority) ? unboundExecutor : standardExecutor;
    }
}
