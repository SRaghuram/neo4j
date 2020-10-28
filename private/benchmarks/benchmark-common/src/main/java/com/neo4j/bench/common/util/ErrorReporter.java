/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.TestRunError;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.joining;

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
        switch ( errorPolicy )
        {
        case FAIL:
            throw new RuntimeException( "Error reported and rethrown", exception );
        case SKIP:
            StringWriter sw = new StringWriter();
            exception.printStackTrace( new PrintWriter( sw ) );
            errors.add( new TestRunError( benchmarkGroup, benchmark, sw.toString() ) );
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

    @Override
    public String toString()
    {
        return errors.isEmpty()
               ? "No errors"
               : errors.stream().map( TestRunError::toString ).collect( joining( "-------------------\n" ) );
    }
}
