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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery5;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

public class ShortQuery5EmbeddedCore_0_1 extends Neo4jShortQuery5<Neo4jConnectionState>
{
    private static final String[] PERSON_PROPERTIES = new String[]{
            Person.ID,
            Person.FIRST_NAME,
            Person.LAST_NAME};

    @Override
    public LdbcShortQuery5MessageCreatorResult execute( Neo4jConnectionState connection,
            LdbcShortQuery5MessageCreator operation ) throws DbException
    {
        Node message = Operators.findNode( connection.db(), Nodes.Message, Message.ID, operation.messageId() );
        Node creator;
        if ( message.hasLabel( Nodes.Comment ) )
        {
            creator = message.getSingleRelationship( Rels.COMMENT_HAS_CREATOR, Direction.OUTGOING ).getEndNode();
        }
        else
        {
            creator = message.getSingleRelationship( Rels.POST_HAS_CREATOR, Direction.OUTGOING ).getEndNode();
        }
        Map<String,Object> creatorProperties = creator.getProperties( PERSON_PROPERTIES );
        return new LdbcShortQuery5MessageCreatorResult(
                (long) creatorProperties.get( Person.ID ),
                (String) creatorProperties.get( Person.FIRST_NAME ),
                (String) creatorProperties.get( Person.LAST_NAME )
        );
    }
}
