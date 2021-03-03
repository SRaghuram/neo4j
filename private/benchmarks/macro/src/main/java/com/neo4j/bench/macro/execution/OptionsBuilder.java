/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.model.options.Edition;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class OptionsBuilder
{
    private Neo4jDeployment neo4jDeployment;
    private Query query;
    private List<ParameterizedProfiler> profilers = new ArrayList<>();
    private List<String> jvmArgs = new ArrayList<>();
    private Planner planner = Planner.DEFAULT;
    private Runtime runtime = Runtime.DEFAULT;
    private Edition edition = Edition.ENTERPRISE;
    private Path outputDir = Paths.get( System.getProperty( "user.dir" ) );
    private Path storeDir;
    private Path neo4jConfigFile;
    private int forks = 1;
    private Jvm jvm;
    private TimeUnit unit = TimeUnit.MILLISECONDS;

    public Options build()
    {
        Objects.requireNonNull( neo4jDeployment );
        Objects.requireNonNull( query );
        Objects.requireNonNull( profilers );
        Objects.requireNonNull( jvmArgs );
        Objects.requireNonNull( planner );
        Objects.requireNonNull( runtime );
        Objects.requireNonNull( edition );
        Objects.requireNonNull( outputDir );
        Objects.requireNonNull( storeDir );
        // neo4j config is allowed to be null
        Objects.requireNonNull( jvm );
        Objects.requireNonNull( unit );

        return new Options(
                neo4jDeployment,
                query,
                profilers,
                jvmArgs,
                planner,
                runtime,
                edition,
                outputDir,
                storeDir,
                Neo4jConfigBuilder.fromFile( neo4jConfigFile ).build(),
                forks,
                jvm,
                unit );
    }

    public OptionsBuilder withNeo4jDeployment( Neo4jDeployment neo4jDeployment )
    {
        this.neo4jDeployment = neo4jDeployment;
        return this;
    }

    public OptionsBuilder withQuery( Query query )
    {
        this.query = query;
        return this;
    }

    public OptionsBuilder withJvmArgs( List<String> jvmArgs )
    {
        this.jvmArgs = jvmArgs;
        return this;
    }

    public OptionsBuilder withPlanner( Planner planner )
    {
        this.planner = planner;
        return this;
    }

    public OptionsBuilder withRuntime( Runtime runtime )
    {
        this.runtime = runtime;
        return this;
    }

    public OptionsBuilder withEdition( Edition edition )
    {
        this.edition = edition;
        return this;
    }

    public OptionsBuilder withOutputDir( Path outputDir )
    {
        this.outputDir = outputDir;
        return this;
    }

    public OptionsBuilder withStoreDir( Path storeDir )
    {
        this.storeDir = storeDir;
        return this;
    }

    public OptionsBuilder withNeo4jConfig( Path neo4jConfigFile )
    {
        this.neo4jConfigFile = neo4jConfigFile;
        return this;
    }

    public OptionsBuilder withForks( int forks )
    {
        this.forks = forks;
        return this;
    }

    public OptionsBuilder withJvm( Jvm jvm )
    {
        this.jvm = jvm;
        return this;
    }

    public OptionsBuilder withUnit( TimeUnit unit )
    {
        this.unit = unit;
        return this;
    }
}
