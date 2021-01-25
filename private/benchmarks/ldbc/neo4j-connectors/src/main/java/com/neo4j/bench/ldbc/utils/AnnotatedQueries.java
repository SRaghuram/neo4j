/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.utils;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;

public interface AnnotatedQueries
{
    AnnotatedQuery queryFor( Operation operation ) throws DbException;
}
