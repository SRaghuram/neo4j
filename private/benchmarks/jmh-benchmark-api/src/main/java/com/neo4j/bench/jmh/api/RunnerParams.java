/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.neo4j.bench.model.util.MapPrinter.prettyPrint;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

/**
 * JMH benchmark parameters that are used by the benchmark harness (as a mechanism to share state), but should not appear in full benchmark names
 */
public final class RunnerParams
{
    static final String PARAM_RUNNER_PARAMS = "runnerParams";
    static final String PARAM_WORK_DIR = "workDir";
    static final String PARAM_RUN_ID = "runId";
    static final String PARAM_PROFILERS = "profilers";

    public static RunnerParams extractFrom( BenchmarkParams benchmarkParams )
    {
        RunnerParams runnerParams = new RunnerParams();
        List<String> runnerParamNames = Arrays.asList( benchmarkParams.getParam( PARAM_RUNNER_PARAMS ).split( "," ) );
        for ( String paramName : runnerParamNames )
        {
            String paramValue = benchmarkParams.getParam( paramName );
            if ( null == paramValue )
            {
                throw new RuntimeException( format( "Could not find Runner Parameters '%s' in JMH benchmark params\n" +
                                                    "Parameters Found: %s", paramName, benchmarkParams.getParamsKeys() ) );
            }
            runnerParams = runnerParams.copyWithParam( paramName, paramValue );
        }
        assertParamExists( runnerParamNames, PARAM_WORK_DIR );
        assertParamExists( runnerParamNames, PARAM_RUN_ID );
        assertParamExists( runnerParamNames, PARAM_PROFILERS );
        return runnerParams;
    }

    private static void assertParamExists( List<String> runnerParamNames, String param )
    {
        if ( !runnerParamNames.contains( param ) )
        {
            throw new RuntimeException( format( "Could not find Runner Parameter '%s' in JMH benchmark params", param ) );
        }
    }

    public static RunnerParams create( Path workDir )
    {
        return new RunnerParams()
                .copyWithParam( PARAM_WORK_DIR, workDir.toAbsolutePath().toString() )
                .copyWithParam( PARAM_RUN_ID, UUID.randomUUID().toString() )
                .copyWithParam( PARAM_PROFILERS, "" );
    }

    private final Map<String,String> runnerParams;

    private RunnerParams()
    {
        this.runnerParams = new HashMap<>();
    }

    public RunnerParams copyWithNewRunId()
    {
        RunnerParams newRunnerParams = new RunnerParams();
        newRunnerParams.runnerParams.putAll( runnerParams );
        newRunnerParams.runnerParams.put( PARAM_RUN_ID, UUID.randomUUID().toString() );
        return newRunnerParams;
    }

    /**
     * {@link Runner} is the only class that (should) use this method, it overwrites the profilers just before starting a benchmark run.
     *
     * @return new {@link RunnerParams} instance, with <code>profilerTypes<code/> updated.
     */
    public RunnerParams copyWithProfilers( List<ParameterizedProfiler> profilers )
    {
        RunnerParams newRunnerParams = new RunnerParams();
        newRunnerParams.runnerParams.putAll( runnerParams );
        newRunnerParams.runnerParams.put( PARAM_PROFILERS, ParameterizedProfiler.serialize( profilers ) );
        return newRunnerParams;
    }

    public RunnerParams copyWithParam( String name, String value )
    {
        if ( runnerParams.containsKey( name ) )
        {
            throw new IllegalStateException( format( "Runner Parameters already contains parameter with name '%s'\n" +
                                                     " * '%s' == '%s'", name, name, runnerParams.get( name ) ) );
        }
        RunnerParams newRunnerParams = new RunnerParams();
        newRunnerParams.runnerParams.putAll( runnerParams );
        newRunnerParams.runnerParams.put( name, value );
        return newRunnerParams;
    }

    public Path workDir()
    {
        return Paths.get( runnerParams.get( PARAM_WORK_DIR ) );
    }

    public String runId()
    {
        return runnerParams.get( PARAM_RUN_ID );
    }

    public List<ProfilerType> profilerTypes()
    {
        return ParameterizedProfiler.profilerTypes( profilers() );
    }

    public List<ParameterizedProfiler> profilers()
    {
        return ParameterizedProfiler.parse( runnerParams.get( PARAM_PROFILERS ) );
    }

    public boolean containsParam( String paramName )
    {
        return runnerParams.containsKey( paramName ) || paramName.equals( PARAM_RUNNER_PARAMS );
    }

    public List<RunnerParam> asList()
    {
        List<RunnerParam> paramsList = runnerParams.entrySet().stream()
                                                   .map( e -> new RunnerParam( e.getKey(), e.getValue() ) )
                                                   .collect( toList() );
        String paramNames = String.join( ",", runnerParams.keySet() );
        paramsList.add( new RunnerParam( PARAM_RUNNER_PARAMS, paramNames ) );
        return paramsList;
    }

    @Override
    public String toString()
    {
        return prettyPrint( runnerParams );
    }

    public static class RunnerParam
    {
        private final String name;
        private final String value;

        private RunnerParam( String name, String value )
        {
            this.name = name;
            this.value = value;
        }

        public String name()
        {
            return name;
        }

        public String value()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return "(" + name + "," + value + ")";
        }
    }
}
