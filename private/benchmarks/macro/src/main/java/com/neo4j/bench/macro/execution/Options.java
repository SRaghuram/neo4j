/**
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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

public class Options
{
    public enum ExecutionMode
    {
        EXECUTE,
        PLAN,
        PROFILE
    }

    private final Query query;
    private final List<ProfilerType> profilers;
    private final List<String> jvmArgs;
    private final Planner planner;
    private final Runtime runtime;
    private final Edition edition;
    private final Path outputDir;
    private final Path storeDir;
    private final Neo4jConfig neo4jConfig;
    private final int forks;
    private final int warmupCount;
    private final int measurementCount;
    private final boolean doPrintResults;
    private final Jvm jvm;
    private final TimeUnit unit;

    Options( Query query,
             List<ProfilerType> profilers,
             List<String> jvmArgs,
             Planner planner,
             Runtime runtime,
             Edition edition,
             Path outputDir,
             Path storeDir,
             Neo4jConfig neo4jConfig,
             int forks,
             int warmupCount,
             int measurementCount,
             boolean doPrintResults,
             Jvm jvm,
             TimeUnit unit )
    {
        this.query = query;
        this.profilers = profilers;
        this.jvmArgs = jvmArgs;
        this.planner = planner;
        this.runtime = runtime;
        this.edition = edition;
        this.outputDir = outputDir;
        this.storeDir = storeDir;
        this.neo4jConfig = neo4jConfig;
        this.forks = forks;
        this.warmupCount = warmupCount;
        this.measurementCount = measurementCount;
        this.doPrintResults = doPrintResults;
        this.jvm = jvm;
        this.unit = unit;
    }

    public Query query()
    {
        return query;
    }

    public List<ProfilerType> profilers()
    {
        return profilers;
    }

    public List<ProfilerType> internalProfilers()
    {
        return profilers.stream().filter( ProfilerType::isInternal ).collect( toList() );
    }

    public List<ProfilerType> externalProfiler()
    {
        return profilers.stream().filter( ProfilerType::isExternal ).collect( toList() );
    }

    public List<String> jvmArgs()
    {
        return jvmArgs;
    }

    public Planner planner()
    {
        return planner;
    }

    public Runtime runtime()
    {
        return runtime;
    }

    public Edition edition()
    {
        return edition;
    }

    public Path outputDir()
    {
        return outputDir;
    }

    public Path storeDir()
    {
        return storeDir;
    }

    public Neo4jConfig neo4jConfig()
    {
        return neo4jConfig;
    }

    public int forks()
    {
        return forks;
    }

    public int warmupCount()
    {
        return warmupCount;
    }

    public int measurementCount()
    {
        return measurementCount;
    }

    public boolean doPrintResults()
    {
        return doPrintResults;
    }

    public Jvm jvm()
    {
        return jvm;
    }

    public TimeUnit unit()
    {
        return unit;
    }
}
