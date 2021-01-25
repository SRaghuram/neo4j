/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

public abstract class Neo4jUpdate4<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcUpdate4AddForum,LdbcNoResult,CONNECTION>
{
    protected static final Integer FORUM_PARAMS = 1;
    protected static final Integer MODERATOR_PERSON_ID = 2;
    protected static final Integer TAG_IDS = 3;
    static final String QUERY_STRING = Resources.fileToString( "/cypher/interactive/update_4.cypher" );
}
