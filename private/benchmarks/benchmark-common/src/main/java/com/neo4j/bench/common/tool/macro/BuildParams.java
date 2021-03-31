/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.tool.macro;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.model.model.BranchAndVersion;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.HashMap;
import java.util.Map;

public class BuildParams
{

    public static final String CMD_NEO4J_COMMIT = "--neo4j-commit";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_COMMIT},
            description = "Commit of Neo4j that benchmark is run against",
            title = "Neo4j Commit" )
    @Required
    private String neo4jCommit;

    public static final String CMD_NEO4J_VERSION = "--neo4j-version";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_VERSION},
            description = "Version of Neo4j that benchmark is run against (e.g., '3.0.2')",
            title = "Neo4j Version" )
    @Required
    private String neo4jVersion;

    public static final String CMD_NEO4J_BRANCH = "--neo4j-branch";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_BRANCH},
            description = "Neo4j branch name",
            title = "Neo4j Branch" )
    @Required
    private String neo4jBranch;

    public static final String CMD_NEO4J_OWNER = "--neo4j-branch-owner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_OWNER},
            description = "Owner of repository containing Neo4j branch",
            title = "Branch Owner" )
    @Required
    private String neo4jBranchOwner;

    public static final String CMD_TEAMCITY_BUILD = "--teamcity-build";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TEAMCITY_BUILD},
            description = "Build number of the TeamCity build that ran the benchmarks",
            title = "TeamCity Build Number" )
    @Required
    private Long teamcityBuild;

    public static final String CMD_PARENT_TEAMCITY_BUILD = "--parent-teamcity-build";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PARENT_TEAMCITY_BUILD},
            description = "Build number of the TeamCity parent build, e.g., Packaging",
            title = "Parent TeamCity Build Number" )
    @Required
    private Long parentBuild;

    public static final String CMD_TRIGGERED_BY = "--triggered-by";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TRIGGERED_BY},
            description = "Specifies user that triggered this build",
            title = "Specifies user that triggered this build" )
    @Required
    private String triggeredBy;

    public BuildParams()
    {
        // default constructor for command line arguments
    }

    public BuildParams( String neo4jCommit,
                        Version neo4jVersion,
                        String neo4jBranch,
                        String neo4jBranchOwner,
                        Long teamcityBuild,
                        Long parentBuild,
                        String triggeredBy )
    {
        this.neo4jCommit = neo4jCommit;
        this.neo4jVersion = neo4jVersion.fullVersion();
        this.neo4jBranch = neo4jBranch;
        this.neo4jBranchOwner = neo4jBranchOwner;
        this.teamcityBuild = teamcityBuild;
        this.parentBuild = parentBuild;
        this.triggeredBy = triggeredBy;
    }

    public BuildParams teamcityBranchAsRealBranch()
    {
        String branch = BranchAndVersion.teamcityBranchToRealBranch( neo4jBranch );
        return new BuildParams( neo4jCommit, neo4jVersion(), branch, neo4jBranchOwner, teamcityBuild, parentBuild, triggeredBy );
    }

    public String neo4jCommit()
    {
        return neo4jCommit;
    }

    public Version neo4jVersion()
    {
        return new Version( neo4jVersion );
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

    public Map<String,String> asMap()
    {
        Map<String,String> map = new HashMap<>();
        map.put( CMD_NEO4J_COMMIT, neo4jCommit );
        map.put( CMD_NEO4J_VERSION, neo4jVersion );
        map.put( CMD_NEO4J_BRANCH, neo4jBranch );
        map.put( CMD_NEO4J_OWNER, neo4jBranchOwner );
        map.put( CMD_TEAMCITY_BUILD, Long.toString( teamcityBuild ) );
        map.put( CMD_PARENT_TEAMCITY_BUILD, Long.toString( parentBuild ) );
        map.put( CMD_TRIGGERED_BY, triggeredBy );
        return map;
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
