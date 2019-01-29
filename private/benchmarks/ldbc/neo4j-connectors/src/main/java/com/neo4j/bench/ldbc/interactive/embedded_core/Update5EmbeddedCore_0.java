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

package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.neo4j.bench.ldbc.Domain.Forum;
import com.neo4j.bench.ldbc.Domain.HasMember;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate5;
import com.neo4j.bench.ldbc.operators.Operators;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class Update5EmbeddedCore_0 extends Neo4jUpdate5<Neo4jConnectionState>
{
    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate5AddForumMembership operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node forum = Operators.findNode( connection.db(), Nodes.Forum, Forum.ID, operation.forumId() );
        Node person = Operators.findNode( connection.db(), Nodes.Person, Person.ID, operation.personId() );
        Relationship membership = forum.createRelationshipTo( person, Rels.HAS_MEMBER );
        membership.setProperty( HasMember.JOIN_DATE, dateUtil.utcToFormat( operation.joinDate().getTime() ) );
        return LdbcNoResult.INSTANCE;
    }
}
