/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTags;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTagsResult;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

import java.util.List;

public abstract class Neo4jSnbBiQuery2<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcSnbBiQuery2TopTags,List<LdbcSnbBiQuery2TopTagsResult>,CONNECTION>
{
    public static final String QUERY_STRING = Resources.fileToString( "/cypher/bi/q2.cypher" );
}
