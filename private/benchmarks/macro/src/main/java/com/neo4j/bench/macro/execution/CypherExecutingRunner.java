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

import static com.neo4j.bench.macro.execution.Options.ExecutionMode.EXECUTE;
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
                            // run at least one 'EXPLAIN' query to populate query caches
                            query.copyWith( EXECUTE ).warmupQueryString(),
                            query.copyWith( EXECUTE ).queryString(),
                            query.benchmarkGroup(),
                            query.benchmark(),
                            query.parameters().create(),
                            forkDirectory,
                            // run at least one 'EXPLAIN' query to populate query caches
                            warmupControl,
                            query.isSingleShot() ? single() : measurementControl );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error running query\n" + query.toString(), e );
        }
    }

    private static boolean isSafeToWarmup( Query query )
    {
        return  // single shot queries can leave the database in a state where running the query again results in a different behavior (e.g., MERGE)
                !query.isSingleShot() ||
                // some single shot queries have companion warmup queries, that are safe to run repeatedly for warmup purposes
                query.hasWarmup();
    }
}
