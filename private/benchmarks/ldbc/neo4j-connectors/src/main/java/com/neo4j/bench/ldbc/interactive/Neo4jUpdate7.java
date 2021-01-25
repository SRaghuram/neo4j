/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

public abstract class Neo4jUpdate7<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcUpdate7AddComment,LdbcNoResult,CONNECTION>
{
    protected static final Integer COMMENT_PARAMS = 1;
    protected static final Integer AUTHOR_PERSON_ID = 2;
    protected static final Integer COUNTRY_ID = 3;
    protected static final Integer REPLY_TO_ID = 4;
    protected static final Integer REPLY_TO_IS_POST = 5;
    protected static final Integer TAG_IDS = 6;
    static final String QUERY_STRING = Resources.fileToString( "/cypher/interactive/update_7.cypher" );
}
