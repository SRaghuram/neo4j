/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircle;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircleResult;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

import java.util.List;

public abstract class Neo4jSnbBiQuery16<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcSnbBiQuery16ExpertsInSocialCircle,List<LdbcSnbBiQuery16ExpertsInSocialCircleResult>,
        CONNECTION>
{
    protected static final String PERSON_ID = "personId";
    protected static final String COUNTRY = "country";
    protected static final String TAG_CLASS = "tagClass";
    protected static final String MIN_PATH_DISTANCE = "minPathDistance";
    protected static final String MAX_PATH_DISTANCE = "maxPathDistance";
    public static final String QUERY_STRING = Resources.fileToString( "/cypher/bi/q16.cypher" );
}
