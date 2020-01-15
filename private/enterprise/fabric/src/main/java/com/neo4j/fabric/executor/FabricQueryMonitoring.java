/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.fabric.transaction.FabricTransactionInfo;

import java.util.Map;

import org.neo4j.common.DependencyResolver;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.kernel.impl.util.MonotonicCounter;
import org.neo4j.memory.OptionalMemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.Clocks;
import org.neo4j.values.virtual.MapValue;

public class FabricQueryMonitoring
{
    private final DependencyResolver dependencyResolver;
    private final Monitors monitors;
    private DatabaseIdRepository databaseIdRepository;

    public FabricQueryMonitoring( DependencyResolver dependencyResolver, Monitors monitors )
    {
        this.dependencyResolver = dependencyResolver;
        this.monitors = monitors;
    }

    QueryMonitor queryMonitor( FabricTransactionInfo transactionInfo, String statement, MapValue params, Thread thread )
    {
        return new QueryMonitor(
                executingQuery( transactionInfo, statement, params, thread ),
                monitors.newMonitor( QueryExecutionMonitor.class )
        );
    }

    static class QueryMonitor
    {
        private final ExecutingQuery executingQuery;
        private final QueryExecutionMonitor monitor;

        private QueryMonitor( ExecutingQuery executingQuery, QueryExecutionMonitor monitor )
        {
            this.executingQuery = executingQuery;
            this.monitor = monitor;
        }

        void start()
        {
            monitor.start( executingQuery );
        }

        void startExecution()
        {
            executingQuery.onCompilationCompleted( null, null, null );
            executingQuery.onExecutionStarted( OptionalMemoryTracker.NONE );
        }

        void endSuccess()
        {
            monitor.endSuccess( executingQuery );
        }

        void endFailure( Throwable failure )
        {
            monitor.endFailure( executingQuery, failure );
        }

        ExecutingQuery getMonitoredQuery()
        {
            return executingQuery;
        }
    }

    private ExecutingQuery executingQuery( FabricTransactionInfo transactionInfo, String statement, MapValue params, Thread thread )
    {

        NamedDatabaseId namedDatabaseId = getDatabaseIdRepository().getByName( transactionInfo.getDatabaseName() ).get();
        return new FabricExecutingQuery( transactionInfo, statement, params, thread, namedDatabaseId );
    }

    private DatabaseIdRepository getDatabaseIdRepository()
    {
        if ( databaseIdRepository == null )
        {
            databaseIdRepository = dependencyResolver.resolveDependency( FabricDatabaseManager.class ).databaseIdRepository();
        }
        return databaseIdRepository;
    }

    private static class FabricExecutingQuery extends ExecutingQuery
    {
        private static MonotonicCounter internalFabricQueryIdGenerator = MonotonicCounter.newAtomicMonotonicCounter();
        private String internalFabricId;

        private FabricExecutingQuery( FabricTransactionInfo transactionInfo, String statement, MapValue params, Thread thread, NamedDatabaseId namedDatabaseId )
        {
            super( -1, //The actual query id should never be used(leaked) to the dbms
                    transactionInfo.getClientConnectionInfo(),
                    namedDatabaseId,
                    transactionInfo.getLoginContext().subject().username(),
                    statement,
                    params,
                    Map.of(),
                    () -> 0L,
                    () -> 0L,
                    () -> 0L,
                    thread.getId(),
                    thread.getName(),
                    Clocks.nanoClock(),
                    CpuClock.NOT_AVAILABLE
            );
            internalFabricId = "F" + internalFabricQueryIdGenerator.incrementAndGet();
        }

        @Override
        public String id()
        {
            return internalFabricId;
        }
    }
}
