/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.process.HasPid;
import com.neo4j.bench.client.profiling.InternalProfiler;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.macro.execution.database.Database;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.execution.measurement.Results.Phase;
import com.neo4j.bench.macro.workload.ParametersReader;
import com.neo4j.bench.macro.workload.QueryString;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class EmbeddedRunner
{
    public static void run( Jvm jvm,
                            Store store,
                            Edition edition,
                            Path neo4jConfig,
                            List<InternalProfiler> profilers,
                            QueryString warmupQueryString,
                            QueryString queryString,
                            BenchmarkGroup benchmarkGroup,
                            Benchmark benchmark,
                            ParametersReader parametersReader,
                            ForkDirectory forkDirectory,
                            MeasurementControl warmupControl,
                            MeasurementControl measurementControl ) throws Exception
    {
        try ( Database database = Database.startWith( store, edition, neo4jConfig ) )
        {
            long pid = HasPid.getPid();

            // completely skip profiling and execution logic if there is no warmup for this query
            warmupControl.reset();
            if ( !warmupControl.isComplete() )
            {
                /*
                 * Notify profilers that warmup is about to begin
                 */
                profilers.forEach( profiler -> profiler.onWarmupBegin( jvm,
                                                                       forkDirectory,
                                                                       pid,
                                                                       benchmarkGroup,
                                                                       benchmark ) );

                /*
                 * Perform warmup
                 */
                try ( Results.ResultsWriter warmupResultsWriter = Results.newWriter( forkDirectory, Phase.WARMUP, NANOSECONDS ) )
                {
                    System.out.println( format( "Performing warmup (%s). Policy: %s", warmupQueryString.executionMode(), warmupControl.description() ) );
                    execute( warmupQueryString, parametersReader, warmupControl, database.db(), warmupResultsWriter );
                }

                /*
                 * Notify profilers that warmup has completed
                 */
                profilers.forEach( profiler -> profiler.onWarmupFinished( jvm,
                                                                          forkDirectory,
                                                                          pid,
                                                                          benchmarkGroup,
                                                                          benchmark ) );
            }
            else
            {
                System.out.println( format( "Skipping warmup. Policy: %s", warmupControl.description() ) );
            }

            /*
             * Notify profilers that measurement is about to begin
             */
            profilers.forEach( profiler -> profiler.onMeasurementBegin( jvm,
                                                                        forkDirectory,
                                                                        pid,
                                                                        benchmarkGroup,
                                                                        benchmark ) );

            /*
             * Perform measurement
             */
            try ( Results.ResultsWriter measurementResultsWriter = Results.newWriter( forkDirectory, Phase.MEASUREMENT, NANOSECONDS ) )
            {
                System.out.println( format( "Performing measurement (%s). Policy: %s", queryString.executionMode(), measurementControl.description() ) );
                execute( queryString, parametersReader, measurementControl, database.db(), measurementResultsWriter );
            }

            /*
             * Notify profilers that measurement has completed
             */
            profilers.forEach( profiler -> profiler.onMeasurementFinished( jvm,
                                                                           forkDirectory,
                                                                           pid,
                                                                           benchmarkGroup,
                                                                           benchmark ) );
        }
    }

    private static void execute( QueryString queryString,
                                 ParametersReader parameters,
                                 MeasurementControl measurementControl,
                                 GraphDatabaseService db,
                                 Results.ResultsWriter resultsWriter ) throws Exception
    {
        CountingResultVisitor resultVisitor = new CountingResultVisitor();
        Supplier<TxWrapper> txCreator = queryString.isPeriodicCommit()
                                        // queries with PERIODIC COMMIT can not be run in an open transaction
                                        ? NoTx::new
                                        : () -> new RealTx( db.beginTx() );
        measurementControl.reset();
        while ( !measurementControl.isComplete() )
        {
            String queryForThisIteration = queryString.value();
            try ( TxWrapper tx = txCreator.get() )
            {
                resultVisitor.reset();
                Map<String,Object> queryParameters = parameters.next();
                long startTimeUtc = System.currentTimeMillis();
                long start = System.nanoTime();

                Result result = db.execute( queryForThisIteration, queryParameters );
                result.accept( resultVisitor );

                long stop = System.nanoTime();
                long duration = stop - start;
                resultsWriter.write( startTimeUtc, startTimeUtc, duration, resultVisitor.count() );
                measurementControl.register( duration );

                tx.success();
            }
        }
    }

    private interface TxWrapper extends AutoCloseable
    {
        void success();
    }

    private static class RealTx implements TxWrapper
    {
        private final Transaction tx;

        private RealTx( Transaction tx )
        {
            this.tx = tx;
        }

        @Override
        public void success()
        {
            tx.success();
        }

        @Override
        public void close()
        {
            tx.close();
        }
    }

    private static class NoTx implements TxWrapper
    {
        @Override
        public void success()
        {
        }

        @Override
        public void close()
        {
        }
    }
}
