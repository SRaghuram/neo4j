/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import com.google.common.collect.Sets;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.options.Runtime;
import com.neo4j.bench.macro.execution.Options.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.neo4j.bench.client.util.BenchmarkUtil.fileToString;
import static com.neo4j.bench.client.util.BenchmarkUtil.sanitize;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

public class Query
{
    // valid JSON config keys
    private static final String NAME = "name";
    private static final String DESCRIPTION = "description";
    private static final String WARMUP_QUERY_FILE = "warmupQueryFile";
    private static final String QUERY_FILE = "queryFile";
    private static final String IS_SINGLE_SHOT = "isSingleShot";
    private static final String HAS_WARMUP = "hasWarmup";
    private static final String IS_MUTATING = "isMutating";
    private static final String PARAMETERS = "parameters";

    static final String DEFAULT_DESCRIPTION = "n/a";
    static final boolean DEFAULT_IS_SINGLE_SHOT = false;
    static final boolean DEFAULT_HAS_WARMUP = true;
    static final boolean DEFAULT_IS_MUTATING = false;
    private static final Set<String> VALID_KEYS = Sets.newHashSet( NAME,
                                                                   DESCRIPTION,
                                                                   WARMUP_QUERY_FILE,
                                                                   QUERY_FILE,
                                                                   IS_SINGLE_SHOT,
                                                                   HAS_WARMUP,
                                                                   IS_MUTATING,
                                                                   PARAMETERS );

    private final String group;
    private final String name;
    private final String description;
    private final QueryString warmupQueryString;
    private final QueryString queryString;
    private final boolean hasWarmup;
    private final boolean isSingleShot;
    private final boolean isMutating;
    private final Parameters parameters;

    // TODO write detailed comments about interplay between: has warmup, is single shot, periodic commit, is mutating

    public static Query from( Map<String,Object> configEntry, String group, Path workloadDir )
    {
        try
        {
            assertConfigHasValidKeys( configEntry );

            Parameters parameters = configEntry.containsKey( PARAMETERS )
                                    ? Parameters.from( (Map<String,Object>) configEntry.get( PARAMETERS ), workloadDir )
                                    : Parameters.empty();
            Path queryFile = workloadDir.resolve( (String) getOrFail( configEntry, QUERY_FILE ) );
            Path warmupQueryFile = configEntry.containsKey( WARMUP_QUERY_FILE )
                                   ? workloadDir.resolve( (String) configEntry.get( WARMUP_QUERY_FILE ) )
                                   : queryFile;

            if ( !Files.exists( warmupQueryFile ) || !Files.exists( queryFile ) )
            {
                throw new WorkloadConfigException( WorkloadConfigError.QUERY_FILE_NOT_FOUND );
            }

            return new Query(
                    group,
                    ((String) getOrFail( configEntry, NAME )).trim(),
                    ((String) configEntry.getOrDefault( DESCRIPTION, DEFAULT_DESCRIPTION )).trim(),
                    StaticQueryString.atDefaults( fileToString( warmupQueryFile ) ),
                    StaticQueryString.atDefaults( fileToString( queryFile ) ),
                    (boolean) configEntry.getOrDefault( IS_SINGLE_SHOT, DEFAULT_IS_SINGLE_SHOT ),
                    (boolean) configEntry.getOrDefault( HAS_WARMUP, DEFAULT_HAS_WARMUP ),
                    (boolean) configEntry.getOrDefault( IS_MUTATING, DEFAULT_IS_MUTATING ),
                    parameters );
        }
        catch ( WorkloadConfigException we )
        {
            throw new WorkloadConfigException( "Error parsing query config: " + configEntry, we.error(), we );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error parsing query config: " + configEntry, e );
        }
    }

    private static void assertConfigHasValidKeys( Map<String,Object> queryConfigs )
    {
        Set<String> actualKeys = Sets.newHashSet( queryConfigs.keySet() );

        if ( !queryConfigs.containsKey( NAME ) )
        {
            throw new WorkloadConfigException( WorkloadConfigError.NO_QUERY_NAME );
        }
        else
        {
            actualKeys.remove( NAME );
        }

        if ( !queryConfigs.containsKey( QUERY_FILE ) )
        {
            throw new WorkloadConfigException( WorkloadConfigError.NO_QUERY_FILE );
        }
        else
        {
            actualKeys.remove( QUERY_FILE );
        }

        Set<String> invalidKeys = actualKeys.stream().filter( key -> !VALID_KEYS.contains( key ) ).collect( toSet() );
        if ( !invalidKeys.isEmpty() )
        {
            throw new WorkloadConfigException( format( "Query config contained unrecognized keys: %s", invalidKeys ),
                                               WorkloadConfigError.INVALID_QUERY_FIELD );
        }

        boolean hasWarmup = (boolean) queryConfigs.getOrDefault( HAS_WARMUP, DEFAULT_HAS_WARMUP );
        if ( !hasWarmup && queryConfigs.containsKey( WARMUP_QUERY_FILE ) )
        {
            throw new RuntimeException( format( "Inconsistent warmup settings in query config, contained %s yet %s=false",
                                                WARMUP_QUERY_FILE, HAS_WARMUP ) );
        }
    }

    Query( String group,
           String name,
           String description,
           QueryString warmupQueryString,
           QueryString queryString,
           boolean isSingleShot,
           boolean hasWarmup,
           boolean isMutating,
           Parameters parameters )
    {
        this.group = group;
        this.name = name;
        this.description = description;
        this.warmupQueryString = warmupQueryString;
        this.queryString = queryString;
        this.parameters = parameters;
        this.isSingleShot = isSingleShot;
        this.hasWarmup = hasWarmup;
        this.isMutating = isMutating;
    }

    public BenchmarkGroup benchmarkGroup()
    {
        return new BenchmarkGroup( group );
    }

    public Benchmark benchmark()
    {
        String simpleName = sanitize( name );
        Map<String,String> params = new HashMap<>();
        params.put( "planner", queryString.planner().name() );
        params.put( "runtime", queryString.runtime().name() );
        params.put( "execution_mode", queryString.executionMode().name() );
        return Benchmark.benchmarkFor( description, simpleName, Benchmark.Mode.LATENCY, params, queryString.value() );
    }

    public String name()
    {
        return name;
    }

    public String description()
    {
        return description;
    }

    public boolean isSingleShot()
    {
        return isSingleShot;
    }

    public boolean hasWarmup()
    {
        return hasWarmup;
    }

    public boolean isMutating()
    {
        return isMutating;
    }

    public QueryString warmupQueryString()
    {
        return warmupQueryString;
    }

    public QueryString queryString()
    {
        return queryString;
    }

    public Query copyWith( Planner newPlanner )
    {
        return new Query( group,
                          name,
                          description,
                          warmupQueryString.copyWith( newPlanner ),
                          queryString.copyWith( newPlanner ),
                          isSingleShot,
                          hasWarmup,
                          isMutating,
                          parameters );
    }

    public Query copyWith( Runtime newRuntime )
    {
        return new Query( group,
                          name,
                          description,
                          warmupQueryString.copyWith( newRuntime ),
                          queryString.copyWith( newRuntime ),
                          isSingleShot,
                          hasWarmup,
                          isMutating,
                          parameters );
    }

    public Query copyWith( ExecutionMode newExecutionMode )
    {
        return new Query( group,
                          name,
                          description,
                          warmupQueryString.copyWith( newExecutionMode ),
                          queryString.copyWith( newExecutionMode ),
                          isSingleShot,
                          hasWarmup,
                          isMutating,
                          parameters );
    }

    public Parameters parameters()
    {
        return parameters;
    }

    private static Object getOrFail( Map<String,Object> queryMap, String key )
    {
        if ( !queryMap.containsKey( key ) )
        {
            throw new RuntimeException( "Query configuration does not contain expected field: " + key + "\n" +
                                        "Query: " + queryMap );
        }
        return queryMap.get( key );
    }

    @Override
    public String toString()
    {
        return "Query\n" +
               "\tgroup           : " + group + "\n" +
               "\tname            : " + name + "\n" +
               "\tdescription     : " + description + "\n" +
               "\tplanner         : " + queryString.planner() + "\n" +
               "\truntime         : " + queryString.runtime() + "\n" +
               "\texecution mode  : " + queryString.executionMode() + "\n" +
               "\tsingle shot     : " + isSingleShot + "\n" +
               "\thas warmup      : " + hasWarmup + "\n" +
               "\tis mutating     : " + isMutating + "\n" +
               "\tparameters      : " + parameters;
    }
}
