/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static java.lang.StrictMath.toIntExact;
import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( "unused" )
public class ReplicationBenchmarkProcedure
{
    @Context
    public DatabaseManager<?> databaseManager;

    @Context
    public SecurityContext securityContext;

    @Context
    public Log log;

    private static final Map<String,DatabaseBenchmark> benchmarks = new HashMap<>();

    @Admin
    @Description( "Start the benchmark." )
    @Procedure( name = "dbms.cluster.benchmark.start", mode = DBMS )
    public synchronized void start( @Name( "dbName" ) String dbName, @Name( "nThreads" ) Long longNThreads, @Name( "blockSize" ) Long longBlockSize )
    {
        if ( benchmarks.containsKey( dbName ) )
        {
            throw new IllegalStateException( "Already running." );
        }

        log.info( "Starting replication benchmark procedure" );

        var blockSize = toIntExact( longBlockSize );
        var nThreads = toIntExact( longNThreads );
        var dbReplicator = getReplicatorFor( dbName );

        var benchmark = DatabaseBenchmark.start( dbName, nThreads, id -> new Worker( dbReplicator, id, blockSize ) );
        benchmarks.put( dbName, benchmark );
    }

    private Replicator getReplicatorFor( String dbName )
    {
        return databaseManager.getDatabaseContext( dbName )
                              .orElseThrow(
                                      () -> new IllegalStateException( String.format( "Could not get context for database '%s'", dbName ) ) )
                              .dependencies()
                              .resolveDependency( Replicator.class );
    }

    @Admin
    @Description( "Stop a running benchmark." )
    @Procedure( name = "dbms.cluster.benchmark.stop", mode = DBMS )
    public synchronized Stream<BenchmarkResult> stop( @Name( "dbName" ) String dbName ) throws InterruptedException
    {
        if ( !benchmarks.containsKey( dbName ) )
        {
            throw new IllegalStateException( String.format( "Not running for database '%s'.", dbName ) );
        }

        log.info( "Stopping replication benchmark procedure for database '%s'", dbName );

        var benchmark = benchmarks.remove( dbName );

        var benchResult = benchmark.stop();
        return Stream.of( benchResult );
    }

    private class Worker implements Runnable
    {
        private final int blockSize;

        private final Replicator replicator;
        private final int id;
        long totalRequests;
        long totalBytes;

        private Thread t;
        private volatile boolean stopped;

        Worker( Replicator replicator, int id, int blockSize )
        {
            this.blockSize = blockSize;
            this.replicator = replicator;
            this.id = id;
        }

        void start()
        {
            t = new Thread( this , "benchmark-worker-" + id );
            t.start();
        }

        @Override
        public void run()
        {
            try
            {
                while ( !stopped )
                {
                    ReplicationResult replicationResult = replicator.replicate( new DummyRequest( new byte[blockSize] ) );

                    if ( replicationResult.outcome() != ReplicationResult.Outcome.APPLIED )
                    {
                        throw new RuntimeException( "Failed to replicate", replicationResult.failure() );
                    }

                    DummyRequest request = replicationResult.stateMachineResult().consume();
                    totalRequests++;
                    totalBytes += request.byteCount();
                }
            }
            catch ( Throwable e )
            {
                log.error( "Worker exception", e );
            }
        }

        void stop()
        {
            stopped = true;
        }

        void join() throws InterruptedException
        {
            t.join();
        }
    }

    private static class DatabaseBenchmark
    {

        private final String dbName;
        private final List<Worker> workers;
        private final long startTime;

        static DatabaseBenchmark start( String dbName, int nThreads, IntFunction<Worker> workerFactory )
        {
            var workers = IntStream.range( 0, nThreads )
                                   .mapToObj( workerFactory )
                                   .collect( Collectors.toList() );
            workers.forEach( Worker::start );
            return new DatabaseBenchmark( dbName, workers, System.currentTimeMillis() );
        }

        private DatabaseBenchmark( String dbName, List<Worker> workers, long startTime )
        {
            this.dbName = dbName;
            this.workers = workers;
            this.startTime = startTime;
        }

        BenchmarkResult stop() throws InterruptedException
        {
            for ( Worker worker : workers )
            {
                worker.stop();
            }

            for ( Worker dbWorker : workers )
            {
                dbWorker.join();
            }

            long runTime = System.currentTimeMillis() - startTime;

            long totalRequests = 0;
            long totalBytes = 0;

            for ( Worker worker : workers )
            {
                totalRequests += worker.totalRequests;
                totalBytes += worker.totalBytes;
            }

            return new BenchmarkResult( dbName, totalRequests, totalBytes, runTime );
        }
    }
}
