/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import java.util.Arrays;

/**
 * This is a special class which simulates OutOfMemory exception.
 */
public class OutOfMemory
{
    public static void main( String[] args )
    {
        byte[] bytes = new byte[16 * 1024 * 1024];
        System.out.println( Arrays.toString( bytes ) );
    }
}
