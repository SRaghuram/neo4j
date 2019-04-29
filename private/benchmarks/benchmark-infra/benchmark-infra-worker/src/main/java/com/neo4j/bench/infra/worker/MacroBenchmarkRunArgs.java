/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.worker;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;

class MacroBenchmarkRunArgs
{
    private String workload;//="${1}"
    private String db;//="${2}"
    private String warmup_count;//="${3}"
    private String measurement_count;//="${4}"
    private String db_edition;//="${5}"
    private String jvm;//="${6}"
    // these arguments are specific to benchmark run environment (worker)
    // and should not be provided by scheduler
    // private String neo4j_config;//="${7}"
    // private String work_dir;//="${8}"
    private String profilers;//="${9}"
    private String forks;//="${10}"
    private String results_path;//="${11}"
    private String time_unit;//="${12}"
    private String results_store_uri;//="${13}"
    private String results_store_user;//="${14}
    private String results_store_password;//="${15}
    private String neo4j_commit;//="${16}"
    private String neo4j_version;//="${17}"
    private String neo4j_branch;//="${18}"
    private String neo4j_branch_owner;//="${19}"
    private String tool_commit;//="${20}"
    private String tool_branch_owner;//="${21}"
    private String tool_branch;//="${22}"
    private String teamcity_build;//="${23}"
    private String parent_teamcity_build;//="${24}"
    private String execution_mode;//="${25}"
    private String jvm_args;//="${26}"
    private String recreate_schema;//="${27}"
    private String planner;//="${28}"
    private String runtime;//="${29}"
    private String triggered_by;//="${30}"
    private String error_policy;//="${31}"
    private String deployment;//="${31}"

    static MacroBenchmarkRunArgs from( List<String> arguments )
    {
        if ( arguments.size() != 30 )
        {
            throw new IllegalArgumentException( format( "unexpected number of run report benchmarks arguments, %d", arguments.size() ) );
        }

        MacroBenchmarkRunArgs args = new MacroBenchmarkRunArgs();
        int idx = 0;
        args.workload = arguments.get( idx++ );//="${1}"
        args.db = arguments.get( idx++ );//="${2}"
        args.warmup_count = arguments.get( idx++ );//="${3}"
        args.measurement_count = arguments.get( idx++ );//="${4}"
        args.db_edition = arguments.get( idx++ );//="${5}"
        args.jvm = arguments.get( idx++ );//="${6}"
        // these arguments are specific to benchmark run environment (worker)
        // and should not be provided by scheduler
        // args.neo4j_config = arguments.get( idx++ );//="${7}"
        // args.work_dir = arguments.get( idx++ );//="${8}"
        args.profilers = arguments.get( idx++ );//="${9}"
        args.forks = arguments.get( idx++ );//="${10}"
        args.results_path = arguments.get( idx++ );//="${11}"
        args.time_unit = arguments.get( idx++ );//="${12}"
        args.results_store_uri = arguments.get( idx++ );//="${13}"
        args.results_store_user = arguments.get( idx++ );//="${14}
        args.results_store_password = arguments.get( idx++ );//="${15}
        args.neo4j_commit = arguments.get( idx++ );//="${16}"
        args.neo4j_version = arguments.get( idx++ );//="${17}"
        args.neo4j_branch = arguments.get( idx++ );//="${18}"
        args.neo4j_branch_owner = arguments.get( idx++ );//="${19}"
        args.tool_commit = arguments.get( idx++ );//="${20}"
        args.tool_branch_owner = arguments.get( idx++ );//="${21}"
        args.tool_branch = arguments.get( idx++ );//="${22}"
        args.teamcity_build = arguments.get( idx++ );//="${23}"
        args.parent_teamcity_build = arguments.get( idx++ );//="${24}"
        args.execution_mode = arguments.get( idx++ );//="${25}"
        args.jvm_args = arguments.get( idx++ );//="${26}"
        args.recreate_schema = arguments.get( idx++ );//="${27}"
        args.planner = arguments.get( idx++ );//="${28}"
        args.runtime = arguments.get( idx++ );//="${29}"
        args.triggered_by = arguments.get( idx++ );//="${30}"
        args.error_policy = arguments.get( idx++ );//="${31}"
        args.deployment = arguments.get( idx++ );
        return args;
    }

    String getTeamcityBuild()
    {
        return teamcity_build;
    }

    String getDb()
    {
        return db;
    }

    String getNeo4jBranch()
    {
        return neo4j_branch;
    }

    String getNeo4jVersion()
    {
        return neo4j_version;
    }

    String getDbEdition()
    {
        return db_edition;
    }

    List<String> toArguments( Path neo4jConfig, Path runDir )
    {
        String[] args = new String[32];
        int idx = 0;
        args[idx++] = workload;//="${1}"
        args[idx++] = db;//="${2}"
        args[idx++] = warmup_count;//="${3}"
        args[idx++] = measurement_count;//="${4}"
        args[idx++] = db_edition;//="${5}"
        args[idx++] = jvm;//="${6}"
        args[idx++] = neo4jConfig.toAbsolutePath().toString();//="${7}"
        args[idx++] = runDir.toAbsolutePath().toString();//="${8}"
        args[idx++] = profilers;//="${9}"
        args[idx++] = forks;//="${10}"
        args[idx++] = results_path;//="${11}"
        args[idx++] = time_unit;//="${12}"
        args[idx++] = results_store_uri;//="${13}"
        args[idx++] = results_store_user;//="${14}
        args[idx++] = results_store_password;//="${15}
        args[idx++] = neo4j_commit;//="${16}"
        args[idx++] = neo4j_version;//="${17}"
        args[idx++] = neo4j_branch;//="${18}"
        args[idx++] = neo4j_branch_owner;//="${19}"
        args[idx++] = tool_commit;//="${20}"
        args[idx++] = tool_branch_owner;//="${21}"
        args[idx++] = tool_branch;//="${22}"
        args[idx++] = teamcity_build;//="${23}"
        args[idx++] = parent_teamcity_build;//="${24}"
        args[idx++] = execution_mode;//="${25}"
        args[idx++] = jvm_args;//="${26}"
        args[idx++] = recreate_schema;//="${27}"
        args[idx++] = planner;//="${28}"
        args[idx++] = runtime;//="${29}"
        args[idx++] = triggered_by;//="${30}"
        args[idx++] = error_policy;//="${31}"
        args[idx++] = deployment;//="{32}"
        return Arrays.asList( args );
    }

}
