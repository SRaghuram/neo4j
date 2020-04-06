/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;


import com.neo4j.bench.common.process.HasPid;

import java.util.Map;

public interface Database extends AutoCloseable, HasPid
{
    /**
     * Executes cypher query and returns row count
     *
     * @param query Cypher query string
     * @param parameters Cypher query parameters
     * @param inTx specifies if query execution should be wrapped in a new transaction
     * @return row count
     */
    int execute( String query, Map<String,Object> parameters, boolean inTx, boolean shouldRollback );
}
