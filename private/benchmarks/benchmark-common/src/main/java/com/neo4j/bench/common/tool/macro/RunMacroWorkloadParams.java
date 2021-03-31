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
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.nio.file.Path;
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

    private MeasurementParams measurementParams;

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

    private BuildParams buildParams;

    // ----------------------------------------------------
    // Run Workload Only
    // ----------------------------------------------------

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
                                   MeasurementParams measurementParams,
                                   int measurementForkCount,
                                   TimeUnit unit,
                                   Runtime runtime,
                                   Planner planner,
                                   ExecutionMode executionMode,
                                   JvmArgs jvmArgs,
                                   boolean recreateSchema,
                                   boolean skipFlameGraphs,
                                   Deployment deployment,
                                   BuildParams buildParams )
    {
        Objects.requireNonNull( queryNames );
        this.workloadName = workloadName;
        this.queryNames = queryNames;
        this.neo4jEdition = neo4jEdition;
        this.jvm = jvm;
        this.profilers = profilers;
        this.measurementParams = measurementParams;
        this.measurementForkCount = measurementForkCount;
        this.unit = unit;
        this.runtime = runtime;
        this.planner = planner;
        this.executionMode = executionMode;
        this.jvmArgs = jvmArgs;
        this.recreateSchema = recreateSchema;
        this.skipFlameGraphs = skipFlameGraphs;
        this.deployment = deployment;
        this.buildParams = buildParams;
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

    public MeasurementParams measurementParams()
    {
        return measurementParams;
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
        return buildParams.neo4jCommit();
    }

    public Version neo4jVersion()
    {
        return buildParams.neo4jVersion();
    }

    public String neo4jBranch()
    {
        return buildParams.neo4jBranch();
    }

    public String neo4jBranchOwner()
    {
        return buildParams.neo4jBranchOwner();
    }

    public Long teamcityBuild()
    {
        return buildParams.teamcityBuild();
    }

    public Long parentBuild()
    {
        return buildParams.parentBuild();
    }

    public String triggeredBy()
    {
        return buildParams.triggeredBy();
    }

    private Map<String,String> asMap()
    {
        Map<String,String> map = new HashMap<>();
        map.put( CMD_WORKLOAD, workloadName );
        map.put( CMD_EDITION, neo4jEdition.name() );
        map.put( CMD_JVM_PATH, jvm.toAbsolutePath().toString() );
        map.put( CMD_PROFILERS, ParameterizedProfiler.serialize( profilers ) );
        map.putAll( measurementParams.asMap() );
        map.put( CMD_FORKS, Integer.toString( measurementForkCount ) );
        map.put( CMD_TIME_UNIT, unit.name() );
        map.put( CMD_RUNTIME, runtime.name() );
        map.put( CMD_PLANNER, planner.name() );
        map.put( CMD_EXECUTION_MODE, executionMode.name() );
        map.put( CMD_JVM_ARGS, jvmArgs.toArgsString() );
        map.put( CMD_NEO4J_DEPLOYMENT, deployment.parsableValue() );
        map.put( CMD_RECREATE_SCHEMA, Boolean.toString( recreateSchema ) );
        map.put( CMD_SKIP_FLAMEGRAPHS, Boolean.toString( skipFlameGraphs ) );
        map.putAll( buildParams.asMap() );
        map.put( CMD_QUERIES, String.join( ",", queryNames ) );
        return map;
    }

    public List<String> asArgs()
    {
        return asMap().entrySet()
                      .stream()
                      .flatMap( this::toArg )
                      .collect( toList() );
    }

    private Stream<String> toArg( Map.Entry<String,String> entry )
    {
        switch ( entry.getValue().toLowerCase() )
        {
        // boolean parameters either exist in args output or do not, they have no value. filter out boolean parameters that have value 'false'
        case "false":
            return Stream.empty();
        case "true":
            return Stream.of( entry.getKey() );
        default:
            return Stream.of( entry.getKey(), entry.getValue() );
        }
    }

    public RunMacroWorkloadParams withQueryNames( List<String> queryNames )
    {
        return new RunMacroWorkloadParams( workloadName,
                                           queryNames,
                                           neo4jEdition,
                                           jvm,
                                           profilers,
                                           measurementParams,
                                           measurementForkCount,
                                           unit,
                                           runtime,
                                           planner,
                                           executionMode,
                                           jvmArgs,
                                           recreateSchema,
                                           skipFlameGraphs,
                                           deployment,
                                           buildParams );
    }

    public RunMacroWorkloadParams withDeployment( Deployment deployment )
    {
        return new RunMacroWorkloadParams( workloadName,
                                           queryNames,
                                           neo4jEdition,
                                           jvm,
                                           profilers,
                                           measurementParams,
                                           measurementForkCount,
                                           unit,
                                           runtime,
                                           planner,
                                           executionMode,
                                           jvmArgs,
                                           recreateSchema,
                                           skipFlameGraphs,
                                           deployment,
                                           buildParams );
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

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this, ToStringStyle.SHORT_PREFIX_STYLE );
    }
}
