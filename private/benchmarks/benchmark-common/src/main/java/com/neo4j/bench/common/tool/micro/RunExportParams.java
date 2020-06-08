/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.tool.micro;

import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.common.util.ErrorReporter;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.File;
import java.nio.file.Paths;

import static com.neo4j.bench.model.options.Edition.ENTERPRISE;

public class RunExportParams
{
    public static final String CMD_JSON_PATH = "--json_path";
    private File jsonPath;

    public static final String CMD_NEO4J_COMMIT = "--neo4j_commit";
    private String neo4jCommit;

    public static final String CMD_NEO4J_VERSION = "--neo4j_version";
    private String neo4jVersion;

    public static final String CMD_NEO4J_EDITION = "--neo4j_edition";
    private Edition neo4jEdition = ENTERPRISE;

    public static final String CMD_NEO4J_BRANCH = "--neo4j_branch";
    private String neo4jBranch;

    public static final String CMD_BRANCH_OWNER = "--branch_owner";
    private String neo4jBranchOwner;

    public static final String CMD_NEO4J_CONFIG = "--neo4j_config";
    private File neo4jConfigFile;

    public static final String CMD_TOOL_COMMIT = "--tool_commit";
    private String toolCommit;

    public static final String CMD_TOOL_OWNER = "--tool_branch_owner";
    private String toolOwner = "neo-technology";

    public static final String CMD_TOOL_BRANCH = "--tool_branch";
    private String toolBranch = neo4jVersion;

    public static final String CMD_TEAMCITY_BUILD = "--teamcity_build";
    private Long build;

    public static final String CMD_PARENT_TEAMCITY_BUILD = "--parent_teamcity_build";
    private Long parentBuild;

    public static final String CMD_JVM_ARGS = "--jvm_args";
    private String jvmArgsString = "";

    public static final String CMD_BENCHMARK_CONFIG = "--config";
    private File benchConfigFile;

    public static final String CMD_JMH_ARGS = "--jmh";
    private String jmhArgs = "";

    public static final String CMD_PROFILES_DIR = "--profiles-dir";
    private File profilerOutput;

    public static final String CMD_PROFILERS = "--profilers";
    private String parameterizedProfilers = "";

    public static final String CMD_STORES_DIR = "--stores-dir";
    private File storesDir = Paths.get( "benchmark_stores" ).toFile();

    public static final String CMD_ERROR_POLICY = "--error-policy";
    private ErrorReporter.ErrorPolicy errorPolicy = ErrorReporter.ErrorPolicy.SKIP;

    public static final String CMD_JVM_PATH = "--jvm";
    private File jvmFile;

    public static final String CMD_TRIGGERED_BY = "--triggered-by";
    private String triggeredBy;

    public RunExportParams( File jsonPath,
                            String neo4jCommit,
                            String neo4jVersion,
                            Edition neo4jEdition,
                            String neo4jBranch,
                            String neo4jBranchOwner,
                            File neo4jConfigFile,
                            String toolCommit,
                            String toolOwner,
                            String toolBranch,
                            Long build,
                            Long parentBuild,
                            String jvmArgsString,
                            File benchConfigFile,
                            String jmhArgs,
                            File profilerOutput,
                            String profilerNames,
                            File storesDir,
                            ErrorReporter.ErrorPolicy errorPolicy,
                            File jvmFile,
                            String triggeredBy )
    {
        this.jsonPath = jsonPath;
        this.neo4jCommit = neo4jCommit;
        this.neo4jVersion = neo4jVersion;
        this.neo4jEdition = neo4jEdition;
        this.neo4jBranch = neo4jBranch;
        this.neo4jBranchOwner = neo4jBranchOwner;
        this.neo4jConfigFile = neo4jConfigFile;
        this.toolCommit = toolCommit;
        this.toolOwner = toolOwner;
        this.toolBranch = toolBranch;
        this.build = build;
        this.parentBuild = parentBuild;
        this.jvmArgsString = jvmArgsString;
        this.benchConfigFile = benchConfigFile;
        this.jmhArgs = jmhArgs;
        this.profilerOutput = profilerOutput;
        this.parameterizedProfilers = profilerNames;
        this.storesDir = storesDir;
        this.errorPolicy = errorPolicy;
        this.jvmFile = jvmFile;
        this.triggeredBy = triggeredBy;
    }

    public File jsonPath()
    {
        return jsonPath;
    }

    public String neo4jCommit()
    {
        return neo4jCommit;
    }

    public String neo4jVersion()
    {
        return neo4jVersion;
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

    public File neo4jConfigFile()
    {
        return neo4jConfigFile;
    }

    public String toolCommit()
    {
        return toolCommit;
    }

    public String toolOwner()
    {
        return toolOwner;
    }

    public String toolBranch()
    {
        return toolBranch;
    }

    public Long build()
    {
        return build;
    }

    public Long parentBuild()
    {
        return parentBuild;
    }

    public String jvmArgsString()
    {
        return jvmArgsString;
    }

    public File benchConfigFile()
    {
        return benchConfigFile;
    }

    public String jmhArgs()
    {
        return jmhArgs;
    }

    public File profilerOutput()
    {
        return profilerOutput;
    }

    public String parameterizedProfilers()
    {
        return parameterizedProfilers;
    }

    public File storesDir()
    {
        return storesDir;
    }

    public ErrorReporter.ErrorPolicy errorPolicy()
    {
        return errorPolicy;
    }

    public File jvmFile()
    {
        return jvmFile;
    }

    public String triggeredBy()
    {
        return triggeredBy;
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
