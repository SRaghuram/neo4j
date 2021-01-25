/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.security;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;

import org.neo4j.configuration.GraphDatabaseSettings;

public abstract class AbstractSecurityBenchmark extends BaseDatabaseBenchmark
{
    @Override
    public String benchmarkGroup()
    {
        return "Security";
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        return new DataGeneratorConfigBuilder()
            .isReusableStore( false )
            .withNeo4jConfig( Neo4jConfigBuilder.withDefaults().withSetting( GraphDatabaseSettings.auth_enabled, "true" ).build() )
            .build();
    }
}
