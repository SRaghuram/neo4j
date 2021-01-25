/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;

public interface Neo4jQuery<INPUT extends Operation<OUTPUT>, OUTPUT, CONNECTION extends DbConnectionState>
{
    OUTPUT execute( CONNECTION connection, INPUT operation ) throws DbException;
}
