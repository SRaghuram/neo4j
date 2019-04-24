/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.client.model.Parameters;
import com.neo4j.bench.client.process.Pid;
import com.neo4j.bench.client.profiling.InternalProfiler;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.macro.execution.database.Database;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.macro.workload.Query;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.neo4j.bench.macro.execution.Options.ExecutionMode.PLAN;

public class CypherPlanningRunner extends QueryRunner
{
    private final Function<ForkDirectory,Database> databaseCreator;

    CypherPlanningRunner( Function<ForkDirectory,Database> databaseCreator )
    {
        this.databaseCreator = databaseCreator;
    }

    @Override
    protected void run( Jvm jvm,
                        Map<Pid,Parameters> pidParameters,
                        Map<Pid,List<InternalProfiler>> pidProfilers,
                        Query query,
                        ForkDirectory forkDirectory,
                        MeasurementControl warmupControl,
                        MeasurementControl measurementControl )
    {
        try
        {
            try ( Database database = databaseCreator.apply( forkDirectory ) )
            {
                Runner.run( jvm,
                            database,
                            pidParameters,
                            pidProfilers,
                            query.copyWith( PLAN ).queryString(),
                            query.copyWith( PLAN ).queryString(),
                            query.benchmarkGroup(),
                            query.benchmark(),
                            query.parameters().create(),
                            forkDirectory,
                            warmupControl,
                            measurementControl );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error running query\n" + query.toString(), e );
        }
    }
}
