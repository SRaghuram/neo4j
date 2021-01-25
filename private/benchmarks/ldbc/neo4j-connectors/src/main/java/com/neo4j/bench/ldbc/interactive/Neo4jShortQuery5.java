/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

public abstract class Neo4jShortQuery5<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcShortQuery5MessageCreator,LdbcShortQuery5MessageCreatorResult,CONNECTION>
{
    protected static final Integer MESSAGE_ID = 1;
    static final String QUERY_STRING = Resources.fileToString( "/cypher/interactive/short_5.cypher" );
}
