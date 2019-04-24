/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BenchmarkArgs
{

    private final List<String> parameters;
    private final URI workerArtifactUri;

    public BenchmarkArgs( List<String> parameters, URI workerArtifactUri )
    {
        this.parameters = parameters;
        this.workerArtifactUri = workerArtifactUri;
    }

    // TOOD: get rid of this constructor,
    // this is just bad to have optional field
    public BenchmarkArgs( List<String> parameters )
    {
        this.parameters = parameters;
        this.workerArtifactUri = null;
    }

    public String getTeamcityBuild()
    {
        return toJobParameters().get("teamcity_build");
    }

    public String getDb()
    {
        return parameters.get( 1 );
    }

    public String getNeo4jBranch()
    {
        return toJobParameters().get("neo4j_branch");
    }

    public String getNeo4jVersion()
    {
        return toJobParameters().get("neo4j_version");
    }

    private Map<String,String> toJobParameters()
    {
        Map<String,String> jobParameters = new HashMap<>();
        // push all positional arguments into map,
        // we should not never ever use positional arguments,
        // period, this is evil, in final version we should first
        // fix run benchmark scripts,
        // this is endless source of errors
        int idx = 0;
        jobParameters.put( "warmup_count", parameters.get( idx++ ) );
        jobParameters.put( "measurement_count", parameters.get( idx++ ) );
        jobParameters.put( "db_edition", parameters.get( idx++ ) );
        jobParameters.put( "jvm", parameters.get( idx++ ) );
        // we should not pass these arguments to jobs
        // jobParameters.put( "neo4j_config", parameters.get( idx++ ) );
        // jobParameters.put( "work_dir", parameters.get( idx++ ) );
        jobParameters.put( "profilers", parameters.get( idx++ ) );
        jobParameters.put( "forks", parameters.get( idx++ ) );
        jobParameters.put( "results_path", parameters.get( idx++ ) );
        jobParameters.put( "time_unit", parameters.get( idx++ ) );
        jobParameters.put( "results_store_uri", parameters.get( idx++ ) );
        jobParameters.put( "results_store_user", parameters.get( idx++ ) );
        jobParameters.put( "results_store_password", parameters.get( idx++ ) );
        jobParameters.put( "neo4j_commit", parameters.get( idx++ ) );
        jobParameters.put( "neo4j_version", parameters.get( idx++ ) );
        jobParameters.put( "neo4j_branch", parameters.get( idx++ ) );
        jobParameters.put( "neo4j_branch_owner", parameters.get( idx++ ) );
        jobParameters.put( "tool_commit", parameters.get( idx++ ) );
        jobParameters.put( "tool_branch_owner", parameters.get( idx++ ) );
        jobParameters.put( "tool_branch", parameters.get( idx++ ) );
        jobParameters.put( "teamcity_build", parameters.get( idx++ ) );
        jobParameters.put( "parent_teamcity_build", parameters.get( idx++ ) );
        jobParameters.put( "execution_mode", parameters.get( idx++ ) );
        jobParameters.put( "jvm_args", parameters.get( idx++ ) );
        jobParameters.put( "recreate_schema", parameters.get( idx++ ) );
        jobParameters.put( "planner", parameters.get( idx++ ) );
        jobParameters.put( "runtime", parameters.get( idx++ ) );
        jobParameters.put( "triggered_by", parameters.get( idx++ ) );
        jobParameters.put( "error_policy", parameters.get( idx++ ) );
        return jobParameters;
    }

    public Map<String,String> toJobParameters(
            String workload,
            String db )
    {
        Map<String,String> jobParameters = new HashMap<>();
        jobParameters.put( "workerArtifactUri", workerArtifactUri.toString() );
        jobParameters.put( "workload", workload );
        jobParameters.put( "db", db );
        jobParameters.putAll( toJobParameters() );
        return jobParameters;
    }

}
