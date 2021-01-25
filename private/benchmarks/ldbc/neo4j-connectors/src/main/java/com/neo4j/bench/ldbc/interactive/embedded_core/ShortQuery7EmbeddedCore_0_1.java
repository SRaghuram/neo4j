/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery7;
import com.neo4j.bench.ldbc.operators.Operators;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;

public class ShortQuery7EmbeddedCore_0_1 extends Neo4jShortQuery7<Neo4jConnectionState>
{
    private static final DescendingCreationDateDescendingAuthorIdComparator
            DESCENDING_CREATION_DATE_DESCENDING_AUTHOR_ID_COMPARATOR =
            new DescendingCreationDateDescendingAuthorIdComparator();

    private static final String[] MESSAGE_PROPERTIES = new String[]{
            Message.ID,
            Message.CONTENT,
            Message.CREATION_DATE};

    private static final String[] PERSON_PROPERTIES = new String[]{
            Person.ID,
            Person.FIRST_NAME,
            Person.LAST_NAME};

    @Override
    public List<LdbcShortQuery7MessageRepliesResult> execute( Neo4jConnectionState connection,
            LdbcShortQuery7MessageReplies operation ) throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node message = Operators.findNode( connection.getTx(), Nodes.Message, Message.ID, operation.messageId() );
        Node messageAuthor;
        if ( message.hasLabel( Nodes.Comment ) )
        {
            messageAuthor = message.getSingleRelationship( Rels.COMMENT_HAS_CREATOR, Direction.OUTGOING ).getEndNode();
        }
        else
        {
            messageAuthor = message.getSingleRelationship( Rels.POST_HAS_CREATOR, Direction.OUTGOING ).getEndNode();
        }

        try ( KnowsCache messageAuthorKnowsCache = new KnowsCache( messageAuthor ) )
        {
            List<LdbcShortQuery7MessageRepliesResult> results = new ArrayList<>();
            for ( Relationship replyOf : message
                    .getRelationships( Direction.INCOMING, Rels.REPLY_OF_POST, Rels.REPLY_OF_COMMENT ) )
            {
                Node replyComment = replyOf.getStartNode();
                Node replyCommentAuthor = replyComment.getSingleRelationship(
                        Rels.COMMENT_HAS_CREATOR,
                        Direction.OUTGOING ).getEndNode();
                Map<String,Object> replyCommentAuthorProperties = replyCommentAuthor.getProperties( PERSON_PROPERTIES );
                Map<String,Object> replyCommentProperties = replyComment.getProperties( MESSAGE_PROPERTIES );
                results.add(
                        new LdbcShortQuery7MessageRepliesResult(
                                (long) replyCommentProperties.get( Message.ID ),
                                (String) replyCommentProperties.get( Message.CONTENT ),
                                dateUtil.formatToUtc( (long) replyCommentProperties.get( Message.CREATION_DATE ) ),
                                (long) replyCommentAuthorProperties.get( Person.ID ),
                                (String) replyCommentAuthorProperties.get( Person.FIRST_NAME ),
                                (String) replyCommentAuthorProperties.get( Person.LAST_NAME ),
                                messageAuthorKnowsCache.knows( replyCommentAuthor )
                        )
                );
            }

            Collections.sort( results, DESCENDING_CREATION_DATE_DESCENDING_AUTHOR_ID_COMPARATOR );

            return results;
        }
    }

    private static class KnowsCache implements AutoCloseable
    {
        private final Node person;
        private final ResourceIterator<Relationship> knows;
        private boolean knowsIsClosed;
        private final LongSet friends;

        private KnowsCache( Node person )
        {
            this.person = person;
            this.knows = (ResourceIterator<Relationship>) person.getRelationships( Direction.BOTH, Rels.KNOWS ).iterator();
            this.knowsIsClosed = false;
            this.friends = new LongOpenHashSet();
        }

        private boolean knows( Node otherPerson )
        {
            if ( friends.contains( otherPerson.getId() ) )
            {
                return true;
            }
            else
            {
                while ( knows.hasNext() )
                {
                    Node friend = knows.next().getOtherNode( person );
                    friends.add( friend.getId() );
                    if ( friend.equals( otherPerson ) )
                    {
                        return true;
                    }
                }
                return false;
            }
        }

        @Override
        public void close()
        {
            if ( !knowsIsClosed )
            {
                knowsIsClosed = true;
                knows.close();
            }
        }
    }

    private static class DescendingCreationDateDescendingAuthorIdComparator
            implements Comparator<LdbcShortQuery7MessageRepliesResult>
    {
        @Override
        public int compare( LdbcShortQuery7MessageRepliesResult result1, LdbcShortQuery7MessageRepliesResult result2 )
        {
            if ( result1.commentCreationDate() > result2.commentCreationDate() )
            {
                return -1;
            }
            else if ( result1.commentCreationDate() < result2.commentCreationDate() )
            {
                return 1;
            }
            else
            {
                if ( result1.replyAuthorId() > result2.replyAuthorId() )
                {
                    return 1;
                }
                else if ( result1.replyAuthorId() < result2.replyAuthorId() )
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
}
