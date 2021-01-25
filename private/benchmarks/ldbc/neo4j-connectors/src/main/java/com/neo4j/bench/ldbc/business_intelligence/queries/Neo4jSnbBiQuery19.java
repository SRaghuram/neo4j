/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19StrangerInteraction;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19StrangerInteractionResult;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

import java.util.List;

public abstract class Neo4jSnbBiQuery19<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcSnbBiQuery19StrangerInteraction,List<LdbcSnbBiQuery19StrangerInteractionResult>,
        CONNECTION>
{
    protected static final String DATE = "date";
    protected static final String TAG_CLASS1 = "tagClass1";
    protected static final String TAG_CLASS2 = "tagClass1";
    public static final String QUERY_STRING = Resources.fileToString( "/cypher/bi/q19.cypher" );
}
