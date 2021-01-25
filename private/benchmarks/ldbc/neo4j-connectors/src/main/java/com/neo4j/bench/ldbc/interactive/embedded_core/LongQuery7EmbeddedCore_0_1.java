/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.neo4j.bench.ldbc.Domain.Likes;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Post;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery7;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;

public class LongQuery7EmbeddedCore_0_1 extends Neo4jQuery7<Neo4jConnectionState>
{
    @Override
    public List<LdbcQuery7Result> execute( Neo4jConnectionState connection, LdbcQuery7 operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );

        Map<Node,Long> personsLikeTimes = new HashMap<>();
        Map<Node,Node> personsLikedMessages = new HashMap<>();
        for ( Relationship hasCreator : person
                .getRelationships( Direction.INCOMING, Rels.POST_HAS_CREATOR, Rels.COMMENT_HAS_CREATOR ) )
        {
            Node message = hasCreator.getStartNode();
            for ( Relationship likes : message
                    .getRelationships( Direction.INCOMING, Rels.LIKES_POST, Rels.LIKES_COMMENT ) )
            {
                Node friend = likes.getStartNode();
                long likeTime = (long) likes.getProperty( Likes.CREATION_DATE );
                Long friendsMostRecentLike = personsLikeTimes.get( friend );
                if ( null == friendsMostRecentLike || likeTime > friendsMostRecentLike )
                {
                    personsLikeTimes.put( friend, likeTime );
                    personsLikedMessages.put( friend, message );
                }
                else if ( likeTime == friendsMostRecentLike )
                {
                    long messageId = (long) message.getProperty( Message.ID );
                    long friendsMostMessageId =
                            (long) personsLikedMessages.get( friend ).getProperty( Message.ID );
                    if ( messageId < friendsMostMessageId )
                    {
                        personsLikeTimes.put( friend, likeTime );
                        personsLikedMessages.put( friend, message );
                    }
                }
            }
        }

        List<LdbcQuery7PreResult> preResults = new ArrayList<>();
        for ( Map.Entry<Node,Long> personsLikeTime : personsLikeTimes.entrySet() )
        {
            Node liker = personsLikeTime.getKey();
            long likerId = (long) liker.getProperty( Person.ID );
            long likeTime = personsLikeTime.getValue();
            preResults.add(
                    new LdbcQuery7PreResult( liker, likerId, likeTime )
            );
        }

        Collections.sort( preResults, new DescendingLikeTimeThenAscendingLikerIdComparator() );

        List<LdbcQuery7Result> results = new ArrayList<>();
        int i = 0;
        for ( LdbcQuery7PreResult preResult : preResults )
        {
            if ( i < operation.limit() )
            {
                Node liker = preResult.person();
                long likerId = preResult.personId();
                String likerFirstName = (String) liker.getProperty( Person.FIRST_NAME );
                String likerLastName = (String) liker.getProperty( Person.LAST_NAME );
                Node message = personsLikedMessages.get( liker );
                long messageId = (long) message.getProperty( Message.ID );
                String messageContent = (message.hasProperty( Message.CONTENT )) ? (String) message
                        .getProperty( Message.CONTENT ) : (String) message.getProperty( Post.IMAGE_FILE );
                long messageCreationDate = (long) message.getProperty( Message.CREATION_DATE );
                long likeCreationDate = preResult.likeTime();
                Long minutesLatency = TimeUnit.MILLISECONDS.toMinutes(
                        dateUtil.formatToUtc( likeCreationDate ) - dateUtil.formatToUtc( messageCreationDate )
                );
                boolean isNew = !likerKnowsPerson( liker, person );
                results.add(
                        new LdbcQuery7Result(
                                likerId,
                                likerFirstName,
                                likerLastName,
                                dateUtil.formatToUtc( likeCreationDate ),
                                messageId,
                                messageContent,
                                minutesLatency.intValue(),
                                isNew
                        )
                );
                i++;
            }
        }

        return Lists.newArrayList(
                Iterables.limit(
                        results,
                        operation.limit()
                )
        );
    }

    private boolean likerKnowsPerson( Node liker, Node person )
    {
        try ( ResourceIterator<Relationship> knowsRelationships = (ResourceIterator<Relationship>) liker.getRelationships( Rels.KNOWS ).iterator() )
        {
            while ( knowsRelationships.hasNext() )
            {
                Relationship knowsRelationship = knowsRelationships.next();
                Node otherPerson = knowsRelationship.getOtherNode( liker );
                if ( otherPerson.equals( person ) )
                {
                    return true;
                }
            }
        }
        return false;
    }

    public static class DescendingLikeTimeThenAscendingLikerIdComparator implements Comparator<LdbcQuery7PreResult>
    {
        @Override
        public int compare( LdbcQuery7PreResult preResult1, LdbcQuery7PreResult preResult2 )
        {
            if ( preResult1.likeTime() > preResult2.likeTime() )
            {
                return -1;
            }
            else if ( preResult1.likeTime() < preResult2.likeTime() )
            {
                return 1;
            }
            else
            {
                if ( preResult1.personId() > preResult2.personId() )
                {
                    return 1;
                }
                else if ( preResult1.personId() < preResult2.personId() )
                {
                    return -1;
                }
                else
                {
                    return 0;
                }
            }
        }
    }

    private class LdbcQuery7PreResult
    {
        private final Node person;
        private final long personId;
        private final long likeTime;

        private LdbcQuery7PreResult( Node person, long personId, long likeTime )
        {
            this.person = person;
            this.personId = personId;
            this.likeTime = likeTime;
        }

        public Node person()
        {
            return person;
        }

        public long personId()
        {
            return personId;
        }

        public long likeTime()
        {
            return likeTime;
        }
    }
}
