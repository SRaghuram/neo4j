/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14TopThreadInitiators;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14TopThreadInitiatorsResult;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

import java.util.List;

public abstract class Neo4jSnbBiQuery14<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcSnbBiQuery14TopThreadInitiators,List<LdbcSnbBiQuery14TopThreadInitiatorsResult>,CONNECTION>
{
    protected static final String START_DATE = "startDate";
    protected static final String END_DATE = "endDate";
    public static final String QUERY_STRING = Resources.fileToString( "/cypher/bi/q14.cypher" );
}
