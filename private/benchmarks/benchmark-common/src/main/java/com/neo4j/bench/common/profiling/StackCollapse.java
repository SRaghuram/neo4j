/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import java.util.function.BiConsumer;

public interface StackCollapse
{
    void forEachStackTrace( BiConsumer<String,Long> fun );
}
