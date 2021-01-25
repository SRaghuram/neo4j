/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.tool.macro;

import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.model.process.JvmArgs;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class RunMacroWorkloadParams
{
    // -----------------------------------------------------------------------
    // Common: Base Run Workload
    // -----------------------------------------------------------------------

    public static final String CMD_WORKLOAD = "--workload";
    private String workloadName;

    public static final String CMD_QUERIES = "--queries";
    private List<String> queryNames;

    public static final String CMD_EDITION = "--db-edition";
    private Edition neo4jEdition;

    public static final String CMD_JVM_PATH = "--jvm";
    private Path jvm;

    public static final String CMD_PROFILERS = "--profilers";
    private List<ParameterizedProfiler> profilers;

    public static final String CMD_WARMUP = "--warmup-count";
    private int warmupCount;

    public static final String CMD_MEASUREMENT = "--measurement-count";
    private int measurementCount;

    public static final String CMD_MIN_MEASUREMENT_DURATION = "--min-measurement-duration";
    private Duration minMeasurementDuration;

    public static final String CMD_MAX_MEASUREMENT_DURATION = "--max-measurement-duration";
    private Duration maxMeasurementDuration;

    public static final String CMD_FORKS = "--forks";
    private int measurementForkCount;

    public static final String CMD_TIME_UNIT = "--time-unit";
    private TimeUnit unit;

    public static final String CMD_RUNTIME = "--runtime";
    private Runtime runtime;

    public static final String CMD_PLANNER = "--planner";
    private Planner planner;

    public static final String CMD_EXECUTION_MODE = "--execution-mode";
    private ExecutionMode executionMode;

    public static final String CMD_ERROR_POLICY = "--error-policy";

    public static final String CMD_JVM_ARGS = "--jvm-args";
    private JvmArgs jvmArgs;

    public static final String CMD_NEO4J_DEPLOYMENT = "--neo4j-deployment";
    private Deployment deployment;

    public static final String CMD_RECREATE_SCHEMA = "--recreate-schema";
    private boolean recreateSchema;

    public static final String CMD_SKIP_FLAMEGRAPHS = "--skip-flamegraphs";
    private boolean skipFlameGraphs;

    // -----------------------------------------------------------------------
    // Common: Result Client Report Results Args
    // -----------------------------------------------------------------------

    public static final String CMD_NEO4J_COMMIT = "--neo4j-commit";
    private String neo4jCommit;

    public static final String CMD_NEO4J_VERSION = "--neo4j-version";
    private Version neo4jVersion;

    public static final String CMD_NEO4J_BRANCH = "--neo4j-branch";
    private String neo4jBranch;

    public static final String CMD_NEO4J_OWNER = "--neo4j-branch-owner";
    private String neo4jBranchOwner;

    public static final String CMD_TEAMCITY_BUILD = "--teamcity-build";
    private Long teamcityBuild;

    public static final String CMD_PARENT_TEAMCITY_BUILD = "--parent-teamcity-build";
    private Long parentBuild;

    public static final String CMD_TRIGGERED_BY = "--triggered-by";
    private String triggeredBy;

    // ----------------------------------------------------
    // Run Workload Only
    // ----------------------------------------------------

    public static final String CMD_NEO4J_CONFIG = "--neo4j-config";
    public static final String CMD_WORK_DIR = "--work-dir";
    public static final String CMD_DB_PATH = "--db-dir";
    public static final String CMD_BATCH_JOB_ID = "--batch-job-id";
    public static final String CMD_DB_NAME = "--db-name";
    public static final String CMD_JOB_PARAMETERS = "--job-parameters";

    // needed for JSON serialization
    private RunMacroWorkloadParams()
    {

    }

    public RunMacroWorkloadParams( String workloadName,
                                   List<String> queryNames,
                                   Edition neo4jEdition,
                                   Path jvm,
                                   List<ParameterizedProfiler> profilers,
                                   int warmupCount,
                                   int measurementCount,
                                   Duration minMeasurementDuration,
                                   Duration maxMeasurementDuration,
                                   int measurementForkCount,
                                   TimeUnit unit,
                                   Runtime runtime,
                                   Planner planner,
                                   ExecutionMode executionMode,
                                   JvmArgs jvmArgs,
                                   boolean recreateSchema,
                                   boolean skipFlameGraphs,
                                   Deployment deployment,
                                   // -----------------------------------------------------------------------
                                   // Result Client Report Results Args
                                   // -----------------------------------------------------------------------
                                   String neo4jCommit,
                                   Version neo4jVersion,
                                   String neo4jBranch,
                                   String neo4jBranchOwner,
                                   Long teamcityBuild,
                                   Long parentBuild,
                                   String triggeredBy )
    {
        Objects.requireNonNull( queryNames );
        this.workloadName = workloadName;
        this.queryNames = queryNames;
        this.neo4jEdition = neo4jEdition;
        this.jvm = jvm;
        this.profilers = profilers;
        this.warmupCount = warmupCount;
        this.measurementCount = measurementCount;
        this.minMeasurementDuration = minMeasurementDuration;
        this.maxMeasurementDuration = maxMeasurementDuration;
        this.measurementForkCount = measurementForkCount;
        this.unit = unit;
        this.runtime = runtime;
        this.planner = planner;
        this.executionMode = executionMode;
        this.jvmArgs = jvmArgs;
        this.recreateSchema = recreateSchema;
        this.skipFlameGraphs = skipFlameGraphs;
        this.deployment = deployment;
        // -----------------------------------------------------------------------
        // Result Client Report Results Args
        // -----------------------------------------------------------------------
        this.neo4jCommit = neo4jCommit;
        this.neo4jVersion = neo4jVersion;
        this.neo4jBranch = neo4jBranch;
        this.neo4jBranchOwner = neo4jBranchOwner;
        this.teamcityBuild = teamcityBuild;
        this.parentBuild = parentBuild;
        this.triggeredBy = triggeredBy;
    }

    public String workloadName()
    {
        return workloadName;
    }

    public List<String> queryNames()
    {
        return queryNames;
    }

    public Edition neo4jEdition()
    {
        return neo4jEdition;
    }

    public Path jvm()
    {
        return jvm;
    }

    public List<ParameterizedProfiler> profilers()
    {
        return profilers;
    }

    public int warmupCount()
    {
        return warmupCount;
    }

    public int measurementCount()
    {
        return measurementCount;
    }

    public Duration minMeasurementDuration()
    {
        return minMeasurementDuration;
    }

    public Duration maxMeasurementDuration()
    {
        return maxMeasurementDuration;
    }

    public int measurementForkCount()
    {
        return measurementForkCount;
    }

    public TimeUnit unit()
    {
        return unit;
    }

    public Runtime runtime()
    {
        return runtime;
    }

    public Planner planner()
    {
        return planner;
    }

    public ExecutionMode executionMode()
    {
        return executionMode;
    }

    public JvmArgs jvmArgs()
    {
        return jvmArgs;
    }

    public Deployment deployment()
    {
        return deployment;
    }

    public boolean isRecreateSchema()
    {
        return recreateSchema;
    }

    public boolean isSkipFlameGraphs()
    {
        return skipFlameGraphs;
    }

    public String neo4jCommit()
    {
        return neo4jCommit;
    }

    public Version neo4jVersion()
    {
        return neo4jVersion;
    }

    public String neo4jBranch()
    {
        return neo4jBranch;
    }

    public String neo4jBranchOwner()
    {
        return neo4jBranchOwner;
    }

    public Long teamcityBuild()
    {
        return teamcityBuild;
    }

    public Long parentBuild()
    {
        return parentBuild;
    }

    public String triggeredBy()
    {
        return triggeredBy;
    }

    @Override
    public String toString()
    {
        return String.format( "Java             : %s\n" +
                              "JVM args         : %s\n" +
                              "Workload         : %s\n" +
                              "Recreate Schema  : %s\n" +
                              "Edition          : %s\n" +
                              "Profilers        : %s\n" +
                              "Planner          : %s\n" +
                              "Runtime          : %s\n" +
                              "Mode             : %s\n" +
                              "Forks            : %s\n" +
                              "Warmup           : %s\n" +
                              "Measure          : %s\n",
                              jvm,
                              jvmArgs,
                              workloadName,
                              recreateSchema,
                              neo4jEdition,
                              profilers,
                              planner,
                              runtime,
                              executionMode,
                              measurementForkCount,
                              warmupCount,
                              measurementCount );
    }

    public Map<String,String> asMap()
    {
        Map<String,String> map = new HashMap<>();
        map.put( CMD_WORKLOAD, workloadName );
        map.put( CMD_EDITION, neo4jEdition.name() );
        map.put( CMD_JVM_PATH, jvm.toAbsolutePath().toString() );
        map.put( CMD_PROFILERS, ParameterizedProfiler.serialize( profilers ) );
        map.put( CMD_WARMUP, Integer.toString( warmupCount ) );
        map.put( CMD_MEASUREMENT, Integer.toString( measurementCount ) );
        map.put( CMD_MIN_MEASUREMENT_DURATION, Long.toString( minMeasurementDuration.getSeconds() ) );
        map.put( CMD_MAX_MEASUREMENT_DURATION, Long.toString( maxMeasurementDuration.getSeconds() ) );
        map.put( CMD_FORKS, Integer.toString( measurementForkCount ) );
        map.put( CMD_TIME_UNIT, unit.name() );
        map.put( CMD_RUNTIME, runtime.name() );
        map.put( CMD_PLANNER, planner.name() );
        map.put( CMD_EXECUTION_MODE, executionMode.name() );
        map.put( CMD_JVM_ARGS, jvmArgs.toArgsString() );
        map.put( CMD_NEO4J_DEPLOYMENT, deployment.parsableValue() );
        map.put( CMD_RECREATE_SCHEMA, Boolean.toString( recreateSchema ) );
        map.put( CMD_SKIP_FLAMEGRAPHS, Boolean.toString( skipFlameGraphs ) );
        map.put( CMD_NEO4J_COMMIT, neo4jCommit );
        map.put( CMD_NEO4J_VERSION, neo4jVersion.fullVersion() );
        map.put( CMD_NEO4J_BRANCH, neo4jBranch );
        map.put( CMD_NEO4J_OWNER, neo4jBranchOwner );
        map.put( CMD_TEAMCITY_BUILD, Long.toString( teamcityBuild ) );
        map.put( CMD_PARENT_TEAMCITY_BUILD, Long.toString( parentBuild ) );
        map.put( CMD_TRIGGERED_BY, triggeredBy );
        return map;
    }

    public List<String> asArgs()
    {
        return asMap().entrySet()
                      .stream()
                      // boolean parameters either exist in args output or do not, they have no value. filter out boolean parameters that have value 'false'
                      .filter( entry -> !entry.getValue().toLowerCase().equals( "false" ) )
                      .flatMap( entry -> entry.getValue().toLowerCase().equals( "true" )
                                         ? Stream.of( entry.getKey() )
                                         : Stream.of( entry.getKey(), entry.getValue() ) )
                      .collect( toList() );
    }

    public RunMacroWorkloadParams setQueryNames( List<String> queryNames )
    {
        return new RunMacroWorkloadParams(
                workloadName,
                queryNames,
                neo4jEdition,
                jvm,
                profilers,
                warmupCount,
                measurementCount,
                minMeasurementDuration,
                maxMeasurementDuration,
                measurementForkCount,
                unit,
                runtime,
                planner,
                executionMode,
                jvmArgs,
                recreateSchema,
                skipFlameGraphs,
                deployment,
                neo4jCommit,
                neo4jVersion,
                neo4jBranch,
                neo4jBranchOwner,
                teamcityBuild,
                parentBuild,
                triggeredBy );
    }

    @Override
    public boolean equals( Object that )
    {
        return EqualsBuilder.reflectionEquals( this, that );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }
}
