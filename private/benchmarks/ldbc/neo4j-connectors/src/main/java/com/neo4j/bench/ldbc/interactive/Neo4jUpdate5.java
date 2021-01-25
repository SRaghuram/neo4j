/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

public abstract class Neo4jUpdate5<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcUpdate5AddForumMembership,LdbcNoResult,CONNECTION>
{
    protected static final Integer FORUM_ID = 1;
    protected static final Integer PERSON_ID = 2;
    protected static final Integer CREATION_DATE = 3;
    static final String QUERY_STRING = Resources.fileToString( "/cypher/interactive/update_5.cypher" );
}
