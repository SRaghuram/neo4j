/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.TestRunError;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class ErrorReporter
{
    public static String stackTraceToString( Throwable e )
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw );
        e.printStackTrace( pw );
        return sw.toString();
    }

    public enum ErrorPolicy
    {
        SKIP,
        FAIL
    }

    private final ErrorPolicy errorPolicy;
    private final List<TestRunError> errors;

    public ErrorReporter( ErrorPolicy errorPolicy )
    {
        this.errorPolicy = errorPolicy;
        this.errors = new ArrayList<>();
    }

    public void recordOrThrow( Exception exception, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        recordOrThrow( exception, benchmarkGroup.name(), benchmark.name() );
    }

    public void recordOrThrow( Exception exception, String groupName, String benchmarkName )
    {
        switch ( errorPolicy )
        {
        case FAIL:
            throw new RuntimeException( "Error reported and rethrown", exception );
        case SKIP:
            StringWriter sw = new StringWriter();
            exception.printStackTrace( new PrintWriter( sw ) );
            errors.add( new TestRunError( groupName, benchmarkName, sw.toString() ) );
            break;
        default:
            throw new RuntimeException( "Unrecognized error policy: " + errorPolicy );
        }
    }

    public List<TestRunError> errors()
    {
        return errors;
    }

    public ErrorPolicy policy()
    {
        return errorPolicy;
    }
}
