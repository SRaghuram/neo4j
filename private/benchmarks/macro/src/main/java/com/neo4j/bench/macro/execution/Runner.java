/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.InternalProfiler;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.execution.database.Database;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.execution.measurement.Results.Phase;
import com.neo4j.bench.macro.workload.ParametersReader;
import com.neo4j.bench.macro.workload.QueryString;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class Runner
{
    public static void run( Jvm jvm,
                            Database database,
                            Map<Pid,Parameters> pidParameters,
                            Map<Pid,List<InternalProfiler>> pidProfilers,
                            QueryString warmupQueryString,
                            QueryString queryString,
                            BenchmarkGroup benchmarkGroup,
                            Benchmark benchmark,
                            ParametersReader parametersReader,
                            ForkDirectory forkDirectory,
                            MeasurementControl warmupControl,
                            MeasurementControl measurementControl,
                            boolean shouldRollbackWarmup ) throws Exception
    {
        // completely skip profiling and execution logic if there is no warmup for this query
        warmupControl.reset();
        if ( !warmupControl.isComplete() )
        {
            /*
             * Notify profilers that warmup is about to begin
             */
            for ( Pid pid : pidParameters.keySet() )
            {
                pidProfilers.get( pid ).forEach( profiler -> profiler.onWarmupBegin( jvm,
                                                                                     forkDirectory,
                                                                                     pid,
                                                                                     benchmarkGroup,
                                                                                     benchmark,
                                                                                     pidParameters.get( pid ) ) );
            }

            /*
             * Perform warmup
             */
            try ( Results.ResultsWriter warmupResultsWriter = Results.newWriter( forkDirectory, Phase.WARMUP, NANOSECONDS ) )
            {
                System.out.println( format( "Performing warmup (%s). Policy: %s",
                                            shouldRollbackWarmup ? warmupQueryString.executionMode() + " + ROLLBACK" : warmupQueryString.executionMode(),
                                            warmupControl.description() ) );
                execute( warmupQueryString, parametersReader, warmupControl, database, warmupResultsWriter, shouldRollbackWarmup );
            }

            /*
             * Notify profilers that warmup has completed
             */
            for ( Pid pid : pidParameters.keySet() )
            {
                pidProfilers.get( pid ).forEach( profiler -> profiler.onWarmupFinished( jvm,
                                                                                        forkDirectory,
                                                                                        pid,
                                                                                        benchmarkGroup,
                                                                                        benchmark,
                                                                                        pidParameters.get( pid ) ) );
            }
        }
        else
        {
            System.out.println( format( "Skipping warmup. Policy: %s", warmupControl.description() ) );
        }

        /*
         * Notify profilers that measurement is about to begin
         */
        for ( Pid pid : pidParameters.keySet() )
        {
            pidProfilers.get( pid ).forEach( profiler -> profiler.onMeasurementBegin( jvm,
                                                                                      forkDirectory,
                                                                                      pid,
                                                                                      benchmarkGroup,
                                                                                      benchmark,
                                                                                      pidParameters.get( pid ) ) );
        }

        /*
         * Perform measurement
         */
        try ( Results.ResultsWriter measurementResultsWriter = Results.newWriter( forkDirectory, Phase.MEASUREMENT, NANOSECONDS ) )
        {
            System.out.println( format( "Performing measurement (%s). Policy: %s", queryString.executionMode(), measurementControl.description() ) );
            execute( queryString, parametersReader, measurementControl, database, measurementResultsWriter, false );
        }

        /*
         * Notify profilers that measurement has completed
         */
        for ( Pid pid : pidParameters.keySet() )
        {
            pidProfilers.get( pid ).forEach( profiler -> profiler.onMeasurementFinished( jvm,
                                                                                         forkDirectory,
                                                                                         pid,
                                                                                         benchmarkGroup,
                                                                                         benchmark,
                                                                                         pidParameters.get( pid ) ) );
        }
    }

    private static void execute( QueryString queryString,
                                 ParametersReader parameters,
                                 MeasurementControl measurementControl,
                                 Database db,
                                 Results.ResultsWriter resultsWriter,
                                 boolean shouldRollback ) throws Exception
    {
        boolean executeInTx = !queryString.isPeriodicCommit();
        measurementControl.reset();
        while ( !measurementControl.isComplete() && parameters.hasNext() )
        {
            String queryForThisIteration = queryString.value();
            Map<String,Object> queryParameters = parameters.next();
            long startTimeUtc = System.currentTimeMillis();
            long start = System.nanoTime();

            int rowCount = db.execute( queryForThisIteration, queryParameters, executeInTx, shouldRollback );

            long stop = System.nanoTime();
            long duration = stop - start;
            resultsWriter.write( startTimeUtc, startTimeUtc, duration, rowCount );
            measurementControl.register( duration );
        }
        if ( !measurementControl.isComplete() )
        {
            throw new RuntimeException( "Run finished before it was supposed to, probably because it ran out of parameters" );
        }
    }
}
