/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1PostingSummary;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1PostingSummaryResult;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

import java.util.List;

public abstract class Neo4jSnbBiQuery1<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcSnbBiQuery1PostingSummary,List<LdbcSnbBiQuery1PostingSummaryResult>,CONNECTION>
{
    public static final String QUERY_STRING = Resources.fileToString( "/cypher/bi/q1.cypher" );
}
