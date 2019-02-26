/*
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

import org.neo4j.configuration.GraphDatabaseSettings;

import static com.neo4j.bench.macro.execution.Options.ExecutionMode.PLAN;

public class EmbeddedCypherPlanner implements QueryRunner
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
            Path neo4jConfigFile = forkDirectory.create( "neo4j-planning.conf" );
            neo4jConfig.withSetting( GraphDatabaseSettings.query_cache_size, "0" )
                       .writeAsProperties( neo4jConfigFile );

            EmbeddedRunner.run(
                    jvm,
                    store,
                    edition,
                    neo4jConfigFile,
                    profilers,
                    query.copyWith( PLAN ).queryString(),
                    query.copyWith( PLAN ).queryString(),
                    query.benchmarkGroup(),
                    query.benchmark(),
                    query.parameters().create( forkDirectory ),
                    forkDirectory,
                    warmupControl,
                    measurementControl );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error running query\n" + query.toString(), e );
        }
    }
}
