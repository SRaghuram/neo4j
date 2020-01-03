/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import com.neo4j.bench.common.util.BenchmarkUtil;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

/**
 * JMH benchmark parameters that are used by the benchmark harness (as a mechanism to share state), but should not appear in full benchmark names
 */
public final class RunnerParams
{
    private static final String PARAM_RUNNER_PARAMS = "runnerParams";
    private static final String PARAM_WORK_DIR = "workDir";

    public static RunnerParams extractFrom( BenchmarkParams benchmarkParams )
    {
        RunnerParams runnerParams = new RunnerParams();
        List<String> runnerParamNames = Arrays.asList( benchmarkParams.getParam( PARAM_RUNNER_PARAMS ).split( "," ) );
        for ( String paramName : runnerParamNames )
        {
            String paramValue = benchmarkParams.getParam( paramName );
            if ( null == paramValue )
            {
                throw new RuntimeException( format( "Could not find Runner Parameters '%s' in JMH benchmark params", paramName ) );
            }
            runnerParams.addParam( paramName, paramValue );
        }
        if ( !runnerParamNames.contains( PARAM_WORK_DIR ) )
        {
            throw new RuntimeException( format( "Could not find Runner Parameter '%s' in JMH benchmark params", PARAM_WORK_DIR ) );
        }
        return runnerParams;
    }

    public static RunnerParams create( Path workDir )
    {
        RunnerParams runnerParams = new RunnerParams();
        runnerParams.addParam( PARAM_WORK_DIR, workDir.toAbsolutePath().toString() );
        return runnerParams;
    }

    private final Map<String,String> runnerParams;

    private RunnerParams()
    {
        this.runnerParams = new HashMap<>();
    }

    public Path workDir()
    {
        return Paths.get( runnerParams.get( PARAM_WORK_DIR ) );
    }

    public boolean containsParam( String paramName )
    {
        return runnerParams.containsKey( paramName ) || paramName.equals( PARAM_RUNNER_PARAMS );
    }

    public void addParam( String name, String value )
    {
        if ( runnerParams.containsKey( name ) )
        {
            throw new IllegalStateException( format( "Runner Parameters already contains parameter with name '%s'\n" +
                                                     " * '%s' == '%s'", name, name, runnerParams.get( name ) ) );
        }
        runnerParams.put( name, value );
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
        return BenchmarkUtil.prettyPrint( runnerParams );
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
    }
}
