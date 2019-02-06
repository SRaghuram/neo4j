/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.profiling.InternalProfiler;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.macro.workload.Query;

import java.nio.file.Path;
import java.util.List;

import static com.neo4j.bench.macro.execution.Options.ExecutionMode.EXECUTE;
import static com.neo4j.bench.macro.execution.Options.ExecutionMode.PLAN;
import static com.neo4j.bench.macro.execution.measurement.MeasurementControl.single;

public class EmbeddedCypherRunner implements QueryRunner
{
    @Override
    public void run( Jvm jvm,
                     Store store,
                     Edition edition,
                     Neo4jConfig neo4jConfig,
                     List<InternalProfiler> profilers,
                     Query query,
                     ForkDirectory forkDirectory,
                     MeasurementControl warmupControl,
                     MeasurementControl measurementControl )
    {
        try
        {
            Path neo4jConfigFile = forkDirectory.create( "neo4j-executing.conf" );
            neo4jConfig.writeAsProperties( neo4jConfigFile );

            EmbeddedRunner.run(
                    jvm,
                    store,
                    edition,
                    neo4jConfigFile,
                    profilers,
                    // run at least one 'EXPLAIN' query to populate query caches
                    isSafeToWarmup( query ) ? query.copyWith( EXECUTE ).warmupQueryString() : query.copyWith( PLAN ).queryString(),
                    query.copyWith( EXECUTE ).queryString(),
                    query.benchmarkGroup(),
                    query.benchmark(),
                    query.parameters().create( forkDirectory ),
                    forkDirectory,
                    // run at least one 'EXPLAIN' query to populate query caches
                    isSafeToWarmup( query ) ? warmupControl : single(),
                    query.isSingleShot() ? single() : measurementControl );
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
