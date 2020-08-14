/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class TestRunReport
{
    private final TestRun testRun;
    private final BenchmarkConfig benchmarkConfig;
    private final Set<Project> projects;
    private final Neo4jConfig baseNeo4jConfig;
    private final Environment environment;
    private final BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics;
    private final BenchmarkTool benchmarkTool;
    private final Java java;
    private final List<BenchmarkPlan> benchmarkPlans;
    private final List<TestRunError> errors;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public TestRunReport()
    {
        this(
                new TestRun(),
                new BenchmarkConfig(),
                Sets.newHashSet( new Project() ),
                Neo4jConfig.empty(),
                new Environment( Collections.emptyMap() ),
                new BenchmarkGroupBenchmarkMetrics(),
                new BenchmarkTool(),
                new Java(),
                Lists.newArrayList( new BenchmarkPlan() ),
                Lists.newArrayList() );
    }

    public TestRunReport(
            TestRun testRun,
            BenchmarkConfig benchmarkConfig,
            Set<Project> projects,
            Neo4jConfig baseNeo4jConfig,
            Environment environment,
            BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics,
            BenchmarkTool benchmarkTool,
            Java java,
            List<BenchmarkPlan> benchmarkPlans )
    {
        this(
                testRun,
                benchmarkConfig,
                projects,
                baseNeo4jConfig,
                environment,
                benchmarkGroupBenchmarkMetrics,
                benchmarkTool,
                java,
                benchmarkPlans,
                new ArrayList<>() );
    }

    public TestRunReport(
            TestRun testRun,
            BenchmarkConfig benchmarkConfig,
            Set<Project> projects,
            Neo4jConfig baseNeo4jConfig,
            Environment environment,
            BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics,
            BenchmarkTool benchmarkTool,
            Java java,
            List<BenchmarkPlan> benchmarkPlans,
            List<TestRunError> errors )
    {
        this.testRun = testRun;
        this.benchmarkConfig = benchmarkConfig;
        this.projects = projects;
        this.baseNeo4jConfig = baseNeo4jConfig;
        this.environment = environment;
        this.benchmarkGroupBenchmarkMetrics = benchmarkGroupBenchmarkMetrics;
        this.benchmarkTool = benchmarkTool;
        this.java = java;
        this.benchmarkPlans = benchmarkPlans;
        this.errors = errors;
    }

    public List<BenchmarkGroupBenchmark> benchmarkGroupBenchmarks()
    {
        return benchmarkGroupBenchmarkMetrics.benchmarkGroupBenchmarks();
    }

    public TestRun testRun()
    {
        return testRun;
    }

    public BenchmarkConfig benchmarkConfig()
    {
        return benchmarkConfig;
    }

    public Set<Project> projects()
    {
        return projects;
    }

    public Neo4jConfig baseNeo4jConfig()
    {
        return baseNeo4jConfig;
    }

    public Environment environment()
    {
        return environment;
    }

    public BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics()
    {
        return benchmarkGroupBenchmarkMetrics;
    }

    public BenchmarkTool benchmarkTool()
    {
        return benchmarkTool;
    }

    public Java java()
    {
        return java;
    }

    public List<BenchmarkPlan> benchmarkPlans()
    {
        return benchmarkPlans;
    }

    public List<TestRunError> errors()
    {
        return errors;
    }

    @Override
    public String toString()
    {
        return "TestRunReport{" +
               "testRun=" + testRun +
               ", benchmarkConfig=" + benchmarkConfig +
               ", projects=" + projects +
               ", baseNeo4jConfig=" + baseNeo4jConfig +
               ", environment=" + environment +
               ", benchmarkGroupBenchmarkMetrics=" + benchmarkGroupBenchmarkMetrics +
               ", benchmarkTool=" + benchmarkTool +
               ", java=" + java +
               ", benchmarkPlans=" + benchmarkPlans +
               ", errors=" + errors +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        TestRunReport that = (TestRunReport) o;
        return Objects.equals( testRun, that.testRun ) &&
               Objects.equals( benchmarkConfig, that.benchmarkConfig ) &&
               Objects.equals( projects, that.projects ) &&
               Objects.equals( baseNeo4jConfig, that.baseNeo4jConfig ) &&
               Objects.equals( environment, that.environment ) &&
               Objects.equals( benchmarkGroupBenchmarkMetrics, that.benchmarkGroupBenchmarkMetrics ) &&
               Objects.equals( benchmarkTool, that.benchmarkTool ) &&
               Objects.equals( java, that.java ) &&
               Objects.equals( benchmarkPlans, that.benchmarkPlans ) &&
               Objects.equals( errors, that.errors );
    }

    @Override
    public int hashCode()
    {
        return Objects
                .hash( testRun, benchmarkConfig, projects, baseNeo4jConfig, environment, benchmarkGroupBenchmarkMetrics,
                        benchmarkTool, java, benchmarkPlans, errors );
    }
}
