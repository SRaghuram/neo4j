/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.neo4j.bench.client.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

public abstract class Neo4jShortQuery6<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcShortQuery6MessageForum,LdbcShortQuery6MessageForumResult,CONNECTION>
{
    protected static final Integer MESSAGE_ID = 1;
    static final String QUERY_STRING = Resources.fileToString( "/cypher/interactive/short_6.cypher" );
}
