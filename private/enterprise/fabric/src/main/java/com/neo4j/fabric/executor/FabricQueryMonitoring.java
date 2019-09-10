/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.fabric.transaction.FabricTransactionInfo;

import java.util.Map;
import java.util.function.LongSupplier;

import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.kernel.impl.util.MonotonicCounter;
import org.neo4j.memory.OptionalMemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;
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
            executingQuery.executionStarted( OptionalMemoryTracker.NONE );
        }

        void endSuccess()
        {
            monitor.endSuccess( executingQuery );
        }

        void endFailure( Throwable failure )
        {
            monitor.endFailure( executingQuery, failure );
        }
    }

    private ExecutingQuery executingQuery( FabricTransactionInfo transactionInfo, String statement, MapValue params, Thread thread )
    {
        MonotonicCounter lastQueryId = MonotonicCounter.newAtomicMonotonicCounter();
        long queryId = lastQueryId.incrementAndGet();
        ClientConnectionInfo connectionInfo = transactionInfo.getClientConnectionInfo();
        NamedDatabaseId namedDatabaseId = getDatabaseIdRepository().getByName( transactionInfo.getDatabaseName() ).get();
        String username = transactionInfo.getLoginContext().subject().username();
        Map<String,Object> annotationData = Map.of();
        LongSupplier lockCount = () -> 0L;
        PageCursorTracer cursorCounters = PageCursorTracer.NULL;
        long threadId = thread.getId();
        String threadName = thread.getName();
        SystemNanoClock systemClock = Clocks.nanoClock();
        CpuClock cpuClock = CpuClock.NOT_AVAILABLE;

        return new ExecutingQuery( queryId, connectionInfo, namedDatabaseId, username, statement, params, annotationData, lockCount, cursorCounters,
                threadId, threadName, systemClock, cpuClock );
    }

    private DatabaseIdRepository getDatabaseIdRepository()
    {
        if ( databaseIdRepository == null )
        {
            databaseIdRepository = dependencyResolver.resolveDependency( FabricDatabaseManager.class ).databaseIdRepository();
        }
        return databaseIdRepository;
    }
}
