/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

import java.util.List;

public abstract class Neo4jQuery12<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcQuery12,List<LdbcQuery12Result>,CONNECTION>
{
    protected static final Integer PERSON_ID = 1;
    protected static final Integer TAG_CLASS_NAME = 2;
    protected static final Integer LIMIT = 3;
    static final String QUERY_STRING = Resources.fileToString( "/cypher/interactive/long_12.cypher" );
}
