/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

public class BenchmarkConfigurationException extends RuntimeException
{
    public BenchmarkConfigurationException( String msg )
    {
        super( msg );
    }
}