/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.macro.execution.database.Database;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.common.profiling.assist.InternalProfilerAssist;
import com.neo4j.bench.macro.workload.Query;

import java.util.function.Function;

import static com.neo4j.bench.common.tool.macro.ExecutionMode.PLAN;

public class CypherPlanningRunner extends QueryRunner
{
    private final Function<ForkDirectory,Database> databaseCreator;

    CypherPlanningRunner( Function<ForkDirectory,Database> databaseCreator )
    {
        this.databaseCreator = databaseCreator;
    }

    @Override
    protected void run( InternalProfilerAssist profilerAssist,
                        Query query,
                        ForkDirectory forkDirectory,
                        MeasurementControl warmupControl,
                        MeasurementControl measurementControl )
    {
        try
        {
            try ( Database database = databaseCreator.apply( forkDirectory ) )
            {
                new Runner().run( database,
                                  profilerAssist,
                                  query.copyWith( PLAN ).queryString(),
                                  query.copyWith( PLAN ).queryString(),
                                  query.parameters().create(),
                                  forkDirectory,
                                  warmupControl,
                                  measurementControl,
                                  false,
                                  MeasuringExecutor.toMeasureLatency() );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error running query\n" + query.toString(), e );
        }
    }
}
