/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.execution.database.Database;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.macro.execution.process.InternalProfilerAssist;
import com.neo4j.bench.macro.workload.Query;

import java.util.function.Function;

import static com.neo4j.bench.common.tool.macro.ExecutionMode.EXECUTE;
import static com.neo4j.bench.macro.execution.measurement.MeasurementControl.single;

public class CypherExecutingRunner extends QueryRunner
{
    private final Function<ForkDirectory,Database> databaseCreator;

    CypherExecutingRunner( Function<ForkDirectory,Database> databaseCreator )
    {
        this.databaseCreator = databaseCreator;
    }

    @Override
    protected void run( Jvm jvm,
                        InternalProfilerAssist profilerAssist,
                        Query query,
                        ForkDirectory forkDirectory,
                        MeasurementControl warmupControl,
                        MeasurementControl measurementControl )
    {
        try
        {
            WarmupStrategy warmupStrategy = WarmupStrategy.warmupStrategyFor( query, warmupControl );
            try ( Database database = databaseCreator.apply( forkDirectory ) )
            {
                new Runner().run( jvm,
                                  database,
                                  profilerAssist,
                                  warmupStrategy.warmupQuery(),
                                  query.copyWith( EXECUTE ).queryString(),
                                  query.benchmarkGroup(),
                                  query.benchmark(),
                                  query.parameters().create(),
                                  forkDirectory,
                                  warmupStrategy.warmupControl(),
                                  query.isSingleShot() ? single() : measurementControl,
                                  warmupStrategy.doRollbackOnWarmup(),
                                  MeasuringExecutor.toMeasureLatency() );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error running query\n" + query.toString(), e );
        }
    }
}
