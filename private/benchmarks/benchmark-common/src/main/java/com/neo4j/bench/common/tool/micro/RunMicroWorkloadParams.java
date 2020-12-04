/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.tool.micro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.ErrorReporter;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.File;
import java.util.List;

public class RunMicroWorkloadParams
{

    public static RunMicroWorkloadParams create( String neo4jCommit,
                                                 String neo4jVersion,
                                                 Edition neo4jEdition,
                                                 String neo4jBranch,
                                                 String neo4jBranchOwner,
                                                 File neo4jConfigFile,
                                                 Long build,
                                                 Long parentBuild,
                                                 String jvmArgsString,
                                                 File benchConfigFile,
                                                 String jmhArgs,
                                                 String profilerNames,
                                                 ErrorReporter.ErrorPolicy errorPolicy,
                                                 File jvm,
                                                 String triggeredBy )
    {
        return new RunMicroWorkloadParams(
                neo4jCommit,
                new Version( neo4jVersion ),
                neo4jEdition,
                neo4jBranch,
                neo4jBranchOwner,
                neo4jConfigFile,
                build,
                parentBuild,
                JvmArgs.parse( jvmArgsString ),
                benchConfigFile,
                jmhArgs,
                ProfilerType.deserializeProfilers( profilerNames ),
                errorPolicy,
                jvm,
                triggeredBy );
    }

    public static final String CMD_NEO4J_COMMIT = "--neo4j_commit";
    private final String neo4jCommit;

    public static final String CMD_NEO4J_VERSION = "--neo4j_version";
    private final Version neo4jVersion;

    public static final String CMD_NEO4J_EDITION = "--neo4j_edition";
    private final Edition neo4jEdition;

    public static final String CMD_NEO4J_BRANCH = "--neo4j_branch";
    private final String neo4jBranch;

    public static final String CMD_BRANCH_OWNER = "--branch_owner";
    private final String neo4jBranchOwner;

    public static final String CMD_NEO4J_CONFIG = "--neo4j_config";
    private final File neo4jConfigFile;

    public static final String CMD_TEAMCITY_BUILD = "--teamcity_build";
    private final Long build;

    public static final String CMD_PARENT_TEAMCITY_BUILD = "--parent_teamcity_build";
    private final Long parentBuild;

    public static final String CMD_JVM_ARGS = "--jvm_args";
    private final JvmArgs jvmArgs;

    public static final String CMD_BENCHMARK_CONFIG = "--config";
    private final File benchConfigFile;

    public static final String CMD_JMH_ARGS = "--jmh";
    private final String jmhArgs;

    public static final String CMD_PROFILERS = "--profilers";
    private final List<ProfilerType> profilers;

    public static final String CMD_ERROR_POLICY = "--error-reporter-policy";
    private final ErrorReporter.ErrorPolicy errorPolicy;

    public static final String CMD_JVM_PATH = "--jvm";
    private final File jvm;

    public static final String CMD_TRIGGERED_BY = "--triggered-by";
    private final String triggeredBy;

    @JsonCreator
    public RunMicroWorkloadParams( @JsonProperty( "neo4jCommit" ) String neo4jCommit,
                                   @JsonProperty( "neo4jVersion" ) Version neo4jVersion,
                                   @JsonProperty( "neo4jEdition" ) Edition neo4jEdition,
                                   @JsonProperty( "neo4jBranch" ) String neo4jBranch,
                                   @JsonProperty( "neo4jBranchOwner" ) String neo4jBranchOwner,
                                   @JsonProperty( "neo4jConfigFile" ) File neo4jConfigFile,
                                   @JsonProperty( "build" ) Long build,
                                   @JsonProperty( "parentBuild" ) Long parentBuild,
                                   @JsonProperty( "jvmArgs" ) JvmArgs jvmArgs,
                                   @JsonProperty( "benchConfigFile" ) File benchConfigFile,
                                   @JsonProperty( "jmhArgs" ) String jmhArgs,
                                   @JsonProperty( "profilers" ) List<ProfilerType> profilers,
                                   @JsonProperty( "errorPolicy" ) ErrorReporter.ErrorPolicy errorPolicy,
                                   @JsonProperty( "jvm" ) File jvm,
                                   @JsonProperty( "triggeredBy" ) String triggeredBy )
    {
        this.neo4jCommit = neo4jCommit;
        this.neo4jVersion = neo4jVersion;
        this.neo4jEdition = neo4jEdition;
        this.neo4jBranch = neo4jBranch;
        this.neo4jBranchOwner = neo4jBranchOwner;
        this.neo4jConfigFile = neo4jConfigFile;
        this.build = build;
        this.parentBuild = parentBuild;
        this.jvmArgs = jvmArgs;
        this.benchConfigFile = benchConfigFile;
        this.jmhArgs = jmhArgs;
        this.profilers = profilers;
        this.errorPolicy = errorPolicy;
        this.jvm = jvm;
        this.triggeredBy = triggeredBy;
    }

    public Version neo4jVersion()
    {
        return neo4jVersion;
    }

    public String triggeredBy()
    {
        return triggeredBy;
    }

    public List<ProfilerType> profilers()
    {
        return profilers;
    }

    public JvmArgs jvmArgs()
    {
        return jvmArgs;
    }

    public File neo4jConfigFile()
    {
        return neo4jConfigFile;
    }

    public File benchConfigFile()
    {
        return benchConfigFile;
    }

    public ErrorReporter.ErrorPolicy errorPolicy()
    {
        return errorPolicy;
    }

    public String jmhArgs()
    {
        return jmhArgs;
    }

    public File jvm()
    {
        return jvm;
    }

    public long build()
    {
        return build;
    }

    public long parentBuild()
    {
        return parentBuild;
    }

    public String neo4jCommit()
    {
        return neo4jCommit;
    }

    public Edition neo4jEdition()
    {
        return neo4jEdition;
    }

    public String neo4jBranch()
    {
        return neo4jBranch;
    }

    public String neo4jBranchOwner()
    {
        return neo4jBranchOwner;
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
