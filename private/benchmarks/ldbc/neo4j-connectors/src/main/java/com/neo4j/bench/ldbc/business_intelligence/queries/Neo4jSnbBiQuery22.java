/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22InternationalDialog;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22InternationalDialogResult;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

import java.util.List;

public abstract class Neo4jSnbBiQuery22<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcSnbBiQuery22InternationalDialog,List<LdbcSnbBiQuery22InternationalDialogResult>,
        CONNECTION>
{
    protected static final String COUNTRY1 = "country1";
    protected static final String COUNTRY2 = "country2";
    public static final String QUERY_STRING = Resources.fileToString( "/cypher/bi/q22.cypher" );
}
