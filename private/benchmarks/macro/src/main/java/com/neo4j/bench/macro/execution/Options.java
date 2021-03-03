/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.workload.Query;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Options
{
    private final Neo4jDeployment neo4jDeployment;
    private final Query query;
    private final List<ParameterizedProfiler> profilers;
    private final List<String> jvmArgs;
    private final Planner planner;
    private final Runtime runtime;
    private final Edition edition;
    private final Path outputDir;
    private final Path storeDir;
    private final Neo4jConfig neo4jConfig;
    private final int forks;
    private final Jvm jvm;
    private final TimeUnit unit;

    Options( Neo4jDeployment neo4jDeployment,
             Query query,
             List<ParameterizedProfiler> profilers,
             List<String> jvmArgs,
             Planner planner,
             Runtime runtime,
             Edition edition,
             Path outputDir,
             Path storeDir,
             Neo4jConfig neo4jConfig,
             int forks,
             Jvm jvm,
             TimeUnit unit )
    {
        this.neo4jDeployment = neo4jDeployment;
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
        this.jvm = jvm;
        this.unit = unit;
    }

    public Neo4jDeployment neo4jDeployment()
    {
        return neo4jDeployment;
    }

    public Query query()
    {
        return query;
    }

    public List<ParameterizedProfiler> profilers()
    {
        return profilers;
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

    public Jvm jvm()
    {
        return jvm;
    }

    public TimeUnit unit()
    {
        return unit;
    }
}
