/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

import java.util.List;

public abstract class Neo4jQuery11<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcQuery11,List<LdbcQuery11Result>,CONNECTION>
{
    protected static final Integer PERSON_ID = 1;
    protected static final Integer WORK_FROM_YEAR = 2;
    protected static final Integer COUNTRY_NAME = 3;
    protected static final Integer LIMIT = 4;
    static final String QUERY_STRING = Resources.fileToString( "/cypher/interactive/long_11.cypher" );
}
