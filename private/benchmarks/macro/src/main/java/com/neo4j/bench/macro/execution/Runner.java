/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.macro.execution.database.Database;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.common.profiling.assist.InternalProfilerAssist;
import com.neo4j.bench.macro.workload.ParametersReader;
import com.neo4j.bench.macro.workload.QueryString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

public class Runner
{
    private static final Logger LOG = LoggerFactory.getLogger( Runner.class );

    public void run( Database database,
                     InternalProfilerAssist profilerAssist,
                     QueryString warmupQueryString,
                     QueryString queryString,
                     ParametersReader parametersReader,
                     ForkDirectory forkDirectory,
                     MeasurementControl warmupControl,
                     MeasurementControl measurementControl,
                     boolean shouldRollbackWarmup,
                     MeasuringExecutor measuringExecutor ) throws Exception
    {
        // completely skip profiling and execution logic if there is no warmup for this query
        warmupControl.reset();
        if ( !warmupControl.isComplete() )
        {
            /*
             * Notify profilers that warmup is about to begin
             */
            profilerAssist.onWarmupBegin();

            /*
             * Perform warmup
             */
            LOG.debug( format( "Performing warmup (%s). Policy: %s",
                               shouldRollbackWarmup ? warmupQueryString.executionMode() + " + ROLLBACK" : warmupQueryString.executionMode(),
                               warmupControl.description() ) );
            measuringExecutor.execute( warmupQueryString,
                                       parametersReader,
                                       warmupControl,
                                       database,
                                       forkDirectory,
                                       shouldRollbackWarmup,
                                       RunPhase.WARMUP );

            /*
             * Notify profilers that warmup has completed
             */
            profilerAssist.onWarmupFinished();
        }
        else
        {
            LOG.debug( format( "Skipping warmup. Policy: %s", warmupControl.description() ) );
        }

        /*
         * Notify profilers that measurement is about to begin
         */
        profilerAssist.onMeasurementBegin();

        /*
         * Perform measurement
         */
        LOG.debug( format( "Performing measurement (%s). Policy: %s", queryString.executionMode(), measurementControl.description() ) );
        measuringExecutor.execute( queryString,
                                   parametersReader,
                                   measurementControl,
                                   database,
                                   forkDirectory,
                                   false,
                                   RunPhase.MEASUREMENT );

        /*
         * Notify profilers that measurement has completed
         */
        profilerAssist.onMeasurementFinished();
    }
}
