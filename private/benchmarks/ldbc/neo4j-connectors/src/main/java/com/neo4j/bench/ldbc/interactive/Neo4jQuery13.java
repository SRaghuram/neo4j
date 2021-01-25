/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

public abstract class Neo4jQuery13<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcQuery13,LdbcQuery13Result,CONNECTION>
{
    protected static final Integer PERSON_ID_1 = 1;
    protected static final Integer PERSON_ID_2 = 2;
    static final String QUERY_STRING = Resources.fileToString( "/cypher/interactive/long_13.cypher" );
}
