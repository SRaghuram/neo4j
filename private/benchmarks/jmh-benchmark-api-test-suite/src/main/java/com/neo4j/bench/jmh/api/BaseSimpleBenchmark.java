/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import org.openjdk.jmh.annotations.Param;

public abstract class BaseSimpleBenchmark extends BaseBenchmark
{
    /**
     * This field is only here for the purpose of testing that {@link RunnerParams} works as intended
     */
    @Param( {} )
    public String foo;
}
