/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

public abstract class Neo4jUpdate1<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcUpdate1AddPerson,LdbcNoResult,CONNECTION>
{
    protected static final Integer PERSON_PARAMS = 1;
    protected static final Integer PERSON_CITY_ID = 2;
    protected static final Integer PERSON_TAG_IDS = 3;
    protected static final Integer PERSON_STUDY_ATS = 4;
    protected static final Integer PERSON_WORK_ATS = 5;
    static final String QUERY_STRING = Resources.fileToString( "/cypher/interactive/update_1.cypher" );
}
