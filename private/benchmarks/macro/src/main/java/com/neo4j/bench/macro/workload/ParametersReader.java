/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import java.util.Map;

public interface ParametersReader extends AutoCloseable
{
    boolean hasNext();

    Map<String,Object> next() throws Exception;
}
