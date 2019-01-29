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

package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.neo4j.bench.client.util.Resources;
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
