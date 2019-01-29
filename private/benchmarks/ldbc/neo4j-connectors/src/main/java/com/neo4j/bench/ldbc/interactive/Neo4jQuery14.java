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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.neo4j.bench.client.util.Resources;
import com.neo4j.bench.ldbc.Neo4jQuery;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import static com.neo4j.bench.ldbc.Domain.Rels;

public abstract class Neo4jQuery14<CONNECTION extends DbConnectionState>
        implements Neo4jQuery<LdbcQuery14,List<LdbcQuery14Result>,CONNECTION>
{
    protected static final Integer PERSON_ID_1 = 1;
    protected static final Integer PERSON_ID_2 = 2;
    static final String QUERY_STRING = Resources.fileToString( "/cypher/interactive/long_14.cypher" );

    public static double conversationWeightBetweenPersons(
            Node startPerson,
            Node endPerson,
            Map<Node,Set<Node>> personComments,
            Map<Node,Set<Node>> personPosts,
            RelationshipType[] postHasCreatorRelationshipTypes,
            RelationshipType[] commentHasCreatorRelationshipTypes )
    {
        Set<Node> startPersonPosts = personPosts.get( startPerson );
        if ( null == startPersonPosts )
        {
            startPersonPosts = personMessages( startPerson, postHasCreatorRelationshipTypes );
            personPosts.put( startPerson, startPersonPosts );
        }
        Set<Node> startPersonComments = personComments.get( startPerson );
        if ( null == startPersonComments )
        {
            startPersonComments = personMessages( startPerson, commentHasCreatorRelationshipTypes );
            personComments.put( startPerson, startPersonComments );
        }
        Set<Node> endPersonPosts = personPosts.get( endPerson );
        if ( null == endPersonPosts )
        {
            endPersonPosts = personMessages( endPerson, postHasCreatorRelationshipTypes );
            personPosts.put( endPerson, endPersonPosts );
        }
        Set<Node> endPersonComments = personComments.get( endPerson );
        if ( null == endPersonComments )
        {
            endPersonComments = personMessages( endPerson, commentHasCreatorRelationshipTypes );
            personComments.put( endPerson, endPersonComments );
        }
        double weight = 0;
        for ( Node comment : startPersonComments )
        {
            Node replyOfMessage = comment.getRelationships( Direction.OUTGOING, Rels.REPLY_OF_COMMENT,
                                                            Rels.REPLY_OF_POST ).iterator().next().getEndNode();
            if ( endPersonPosts.contains( replyOfMessage ) )
            {
                weight = weight + 1.0;
            }
            else if ( endPersonComments.contains( replyOfMessage ) )
            {
                weight = weight + 0.5;
            }
        }
        for ( Node comment : endPersonComments )
        {
            Node replyOfMessage = comment.getRelationships( Direction.OUTGOING, Rels.REPLY_OF_COMMENT,
                                                            Rels.REPLY_OF_POST ).iterator().next().getEndNode();
            if ( startPersonPosts.contains( replyOfMessage ) )
            {
                weight = weight + 1.0;
            }
            else if ( startPersonComments.contains( replyOfMessage ) )
            {
                weight = weight + 0.5;
            }
        }
        return weight;
    }

    private static Set<Node> personMessages( Node person, RelationshipType[] messageHasCreatorRelationshipType )
    {
        Set<Node> messages = new HashSet<>();
        for ( Relationship messageHasCreator :
                person.getRelationships( Direction.INCOMING, messageHasCreatorRelationshipType ) )
        {
            Node message = messageHasCreator.getStartNode();
            messages.add( message );
        }
        return messages;
    }

    // TODO get [path] and convert to <persons,[person]> knows matrix
    //
    // TODO get <person,[person]> knows matrix - weights only necessary if persons are friends
    // TODO may actually need <person,[knows]> too/instead
    // TODO calculate weights between persons, as <knows,weight> map
    // TODO return
}
