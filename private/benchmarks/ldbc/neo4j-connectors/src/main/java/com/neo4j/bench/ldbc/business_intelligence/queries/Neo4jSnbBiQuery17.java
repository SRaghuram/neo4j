/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17FriendshipTriangles;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17FriendshipTrianglesResult;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

public abstract class Neo4jSnbBiQuery17<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcSnbBiQuery17FriendshipTriangles,LdbcSnbBiQuery17FriendshipTrianglesResult,CONNECTION>
{
    protected static final String COUNTRY = "country";
    public static final String QUERY_STRING = Resources.fileToString( "/cypher/bi/q17.cypher" );
}
