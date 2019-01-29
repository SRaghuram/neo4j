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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.neo4j.bench.client.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

public abstract class Neo4jUpdate7<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcUpdate7AddComment,LdbcNoResult,CONNECTION>
{
    protected static final Integer COMMENT_PARAMS = 1;
    protected static final Integer AUTHOR_PERSON_ID = 2;
    protected static final Integer COUNTRY_ID = 3;
    protected static final Integer REPLY_TO_ID = 4;
    protected static final Integer REPLY_TO_IS_POST = 5;
    protected static final Integer TAG_IDS = 6;
    static final String QUERY_STRING = Resources.fileToString( "/cypher/interactive/update_7.cypher" );
}
