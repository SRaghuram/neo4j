/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.business_intelligence.queries;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircle;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircleResult;
import com.neo4j.bench.client.util.Resources;
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
