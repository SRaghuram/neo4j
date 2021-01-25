/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

public abstract class Neo4jUpdate6<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcUpdate6AddPost,LdbcNoResult,CONNECTION>
{
    protected static final Integer POST_PARAMS = 1;
    protected static final Integer AUTHOR_PERSON_ID = 2;
    protected static final Integer FORUM_ID = 3;
    protected static final Integer COUNTRY_ID = 4;
    protected static final Integer TAG_IDS = 5;
    static final String QUERY_STRING = Resources.fileToString( "/cypher/interactive/update_6.cypher" );
}
