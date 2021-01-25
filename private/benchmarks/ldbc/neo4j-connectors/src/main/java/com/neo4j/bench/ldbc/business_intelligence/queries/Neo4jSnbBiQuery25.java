/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery25WeightedPaths;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery25WeightedPathsResult;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

import java.util.List;

public abstract class Neo4jSnbBiQuery25<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcSnbBiQuery25WeightedPaths,List<LdbcSnbBiQuery25WeightedPathsResult>,
        CONNECTION>
{
    protected static final String PERSON1_ID = "person1Id";
    protected static final String PERSON2_ID = "person2Id";
    protected static final String START_DATE = "startDate";
    protected static final String END_DATE = "endDate";
    public static final String QUERY_STRING = Resources.fileToString( "/cypher/bi/q25.cypher" );
}
