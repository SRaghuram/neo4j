/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10TagPerson;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10TagPersonResult;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

import java.util.List;

public abstract class Neo4jSnbBiQuery10<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcSnbBiQuery10TagPerson,List<LdbcSnbBiQuery10TagPersonResult>,CONNECTION>
{
    protected static final String TAG = "tag";
    protected static final String DATE = "date";
    public static final String QUERY_STRING = Resources.fileToString( "/cypher/bi/q10.cypher" );
}
