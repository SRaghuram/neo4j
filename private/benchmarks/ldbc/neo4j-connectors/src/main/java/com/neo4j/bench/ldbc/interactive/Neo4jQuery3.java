/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

import java.util.List;

public abstract class Neo4jQuery3<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcQuery3,List<LdbcQuery3Result>,CONNECTION>
{
    protected static final Integer PERSON_ID = 1;
    protected static final Integer COUNTRY_X = 2;
    protected static final Integer COUNTRY_Y = 3;
    protected static final Integer MIN_DATE = 4;
    protected static final Integer MAX_DATE = 5;
    protected static final Integer LIMIT = 6;
    static final String QUERY_STRING = Resources.fileToString( "/cypher/interactive/long_3.cypher" );
}
