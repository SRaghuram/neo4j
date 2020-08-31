/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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

    @JsonCreator
    public TestRunReport(
            @JsonProperty( "testRun" ) TestRun testRun,
            @JsonProperty( "benchmarkConfig" ) BenchmarkConfig benchmarkConfig,
            @JsonProperty( "projects" ) Set<Project> projects,
            @JsonProperty( "baseNeo4jConfig" ) Neo4jConfig baseNeo4jConfig,
            @JsonProperty( "environment" ) Environment environment,
            @JsonProperty( "benchmarkGroupBenchmarkMetrics" ) BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics,
            @JsonProperty( "benchmarkTool" ) BenchmarkTool benchmarkTool,
            @JsonProperty( "java" ) Java java,
            @JsonProperty( "benchmarkPlans" ) List<BenchmarkPlan> benchmarkPlans,
            @JsonProperty( "errors" ) List<TestRunError> errors )
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
