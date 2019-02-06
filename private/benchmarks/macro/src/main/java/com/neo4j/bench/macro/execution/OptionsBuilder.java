/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.options.Runtime;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.macro.workload.Query;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class OptionsBuilder
{
    // TODO error policy

    private Query query;
    private List<ProfilerType> profilers = new ArrayList<>();
    private List<String> jvmArgs = new ArrayList<>();
    private Planner planner = Planner.DEFAULT;
    private Runtime runtime = Runtime.DEFAULT;
    private Edition edition = Edition.ENTERPRISE;
    private Path outputDir = Paths.get( System.getProperty( "user.dir" ) );
    private Path storeDir;
    private Path neo4jConfigFile;
    private int forks = 1;
    private int warmupCount = 1;
    private int measurementCount = 1;
    private boolean doPrintResults = true;
    private Path jvmFile;
    private TimeUnit unit = TimeUnit.MILLISECONDS;

    public Options build()
    {
        Objects.requireNonNull( query );
        Objects.requireNonNull( profilers );
        Objects.requireNonNull( jvmArgs );
        Objects.requireNonNull( planner );
        Objects.requireNonNull( runtime );
        Objects.requireNonNull( edition );
        Objects.requireNonNull( outputDir );
        Objects.requireNonNull( storeDir );
        // neo4j config is allowed to be null
        // jvm is allowed to be null
        Objects.requireNonNull( unit );

        return new Options(
                query,
                profilers,
                jvmArgs,
                planner,
                runtime,
                edition,
                outputDir,
                storeDir,
                Neo4jConfig.fromFile( neo4jConfigFile ),
                forks,
                warmupCount,
                measurementCount,
                doPrintResults,
                Jvm.bestEffort( jvmFile ),
                unit );
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

    public OptionsBuilder withWarmupCount( int warmupCount )
    {
        this.warmupCount = warmupCount;
        return this;
    }

    public OptionsBuilder withMeasurementCount( int measurementCount )
    {
        this.measurementCount = measurementCount;
        return this;
    }

    public OptionsBuilder withPrintResults( boolean doPrintResults )
    {
        this.doPrintResults = doPrintResults;
        return this;
    }

    public OptionsBuilder withJvm( Path jvmFile )
    {
        this.jvmFile = jvmFile;
        return this;
    }

    public OptionsBuilder withUnit( TimeUnit unit )
    {
        this.unit = unit;
        return this;
    }
}
