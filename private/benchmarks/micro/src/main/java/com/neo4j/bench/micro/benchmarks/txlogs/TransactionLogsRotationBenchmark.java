/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.txlogs;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;

import java.io.IOException;
import java.nio.file.Path;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.data.DataGenerator.GraphWriter.TRANSACTIONAL;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;

public class TransactionLogsRotationBenchmark extends AbstractTransactionLogsBenchmark
{
    @ParamValues( allowed = {"true", "false"}, base = {"true", "false"} )
    @Param( {} )
    private String preallocation;

    @Override
    public String description()
    {
        return "Benchmarking transaction logs rotation benchmark";
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        Neo4jConfig neo4jConfig = Neo4jConfigBuilder.empty()
                                                    .withSetting( preallocate_logical_logs, preallocation ).build();
        return new DataGeneratorConfigBuilder()
                .withGraphWriter( TRANSACTIONAL )
                .withNeo4jConfig( neo4jConfig )
                .isReusableStore( false )
                .build();
    }

    @Benchmark
    @BenchmarkMode( value = Mode.SampleTime )
    public Path rotate() throws IOException
    {
        return logFile.rotate();
    }

    public static void main( String... methods )
    {
        run( TransactionLogsRotationBenchmark.class, methods );
    }
}
