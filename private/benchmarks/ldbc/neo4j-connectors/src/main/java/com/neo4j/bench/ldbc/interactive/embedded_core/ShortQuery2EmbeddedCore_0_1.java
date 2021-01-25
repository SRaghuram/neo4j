/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.google.common.collect.MinMaxPriorityQueue;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Post;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery2;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;

public class ShortQuery2EmbeddedCore_0_1 extends Neo4jShortQuery2<Neo4jConnectionState>
{
    private static final DescendingCreationDateAscendingIdComparator DESCENDING_CREATION_DATE_DESCENDING_ID_COMPARATOR =
            new DescendingCreationDateAscendingIdComparator();
    private static final String[] PERSON_PROPERTIES = new String[]{
            Person.ID,
            Person.FIRST_NAME,
            Person.LAST_NAME};

    @Override
    public List<LdbcShortQuery2PersonPostsResult> execute( Neo4jConnectionState connection,
            LdbcShortQuery2PersonPosts operation ) throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );

        MinMaxPriorityQueue<MessageDetails> messages = MinMaxPriorityQueue
                .orderedBy( DESCENDING_CREATION_DATE_DESCENDING_ID_COMPARATOR )
                .maximumSize( operation.limit() )
                .create();

        long minValidCreationDate = Integer.MIN_VALUE;
        boolean atLimit = false;

        for ( Relationship hasCreator : person
                .getRelationships( Direction.INCOMING, Rels.POST_HAS_CREATOR, Rels.COMMENT_HAS_CREATOR ) )
        {
            Node message = hasCreator.getStartNode();
            long messageCreationDate = (long) message.getProperty( Message.CREATION_DATE );
            if ( messageCreationDate >= minValidCreationDate )
            {
                messages.add(
                        new MessageDetails( message, messageCreationDate, hasCreator.isType( Rels.POST_HAS_CREATOR ) )
                );
                if ( atLimit )
                {
                    minValidCreationDate = messages.peekLast().creationDate();
                }
                else if ( messages.size() == operation.limit() )
                {
                    atLimit = true;
                    minValidCreationDate = messages.peekLast().creationDate();
                }
            }
        }

        PostDetailsCache postDetailsCache = new PostDetailsCache();
        OriginalPostCache originalPostCache = new OriginalPostCache();
        List<LdbcShortQuery2PersonPostsResult> result = new ArrayList<>();
        MessageDetails messageDetails;
        while ( null != (messageDetails = messages.poll()) )
        {
            Node post = originalPostCache.getPost( messageDetails.message() );
            PostDetails postDetails = postDetailsCache.getDetailsForPost( post );
            result.add(
                    new LdbcShortQuery2PersonPostsResult(
                            messageDetails.id(),
                            messageDetails.content(),
                            dateUtil.formatToUtc( messageDetails.creationDate() ),
                            postDetails.postId(),
                            postDetails.postAuthorId(),
                            postDetails.postAuthorFirstName(),
                            postDetails.postAuthorLastName()
                    )
            );
        }
        return result;
    }

    private static class AuthorDetails
    {
        private final long id;
        private final String firstName;
        private final String lastName;

        private AuthorDetails( long id, String firstName, String lastName )
        {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
        }

        public long id()
        {
            return id;
        }

        public String firstName()
        {
            return firstName;
        }

        public String lastName()
        {
            return lastName;
        }
    }

    private static class PostDetailsCache
    {
        private final Map<Node,PostDetails> postDetailsMap;
        private final Map<Node,AuthorDetails> authorDetailsMap;

        private PostDetailsCache()
        {
            this.postDetailsMap = new HashMap<>();
            this.authorDetailsMap = new HashMap<>();
        }

        PostDetails getDetailsForPost( Node post )
        {
            PostDetails postDetails = postDetailsMap.get( post );
            if ( null == postDetails )
            {
                long postId = (long) post.getProperty( Message.ID );
                Node postAuthor = post.getSingleRelationship( Rels.POST_HAS_CREATOR, Direction.OUTGOING ).getEndNode();
                AuthorDetails authorDetails = authorDetailsMap.get( postAuthor );
                if ( null == authorDetails )
                {
                    Map<String,Object> postAuthorProperties = postAuthor.getProperties( PERSON_PROPERTIES );
                    authorDetails = new AuthorDetails(
                            (long) postAuthorProperties.get( Person.ID ),
                            (String) postAuthorProperties.get( Person.FIRST_NAME ),
                            (String) postAuthorProperties.get( Person.LAST_NAME )
                    );
                    authorDetailsMap.put( postAuthor, authorDetails );
                }
                postDetails = new PostDetails(
                        postId,
                        authorDetails.id(),
                        authorDetails.firstName(),
                        authorDetails.lastName()
                );
                postDetailsMap.put( post, postDetails );
            }
            return postDetails;
        }
    }

    private static class PostDetails
    {
        private final long postId;
        private final long postAuthorId;
        private final String postAuthorFirstName;
        private final String postAuthorLastName;

        private PostDetails( long postId, long postAuthorId, String postAuthorFirstName, String postAuthorLastName )
        {
            this.postId = postId;
            this.postAuthorId = postAuthorId;
            this.postAuthorFirstName = postAuthorFirstName;
            this.postAuthorLastName = postAuthorLastName;
        }

        public long postId()
        {
            return postId;
        }

        public long postAuthorId()
        {
            return postAuthorId;
        }

        public String postAuthorFirstName()
        {
            return postAuthorFirstName;
        }

        public String postAuthorLastName()
        {
            return postAuthorLastName;
        }
    }

    private static class OriginalPostCache
    {
        private final Map<Node,Node> originalPostMap;

        private OriginalPostCache()
        {
            this.originalPostMap = new HashMap<>();
        }

        private Node getPost( Node message )
        {
            Node post = originalPostMap.get( message );
            if ( null != post )
            {
                // Original message already known: (message:Post)
                return post;
            }
            else
            {
                try ( ResourceIterator<Relationship> replyOfs = (ResourceIterator<Relationship>) message.getRelationships(
                        Direction.OUTGOING,
                        Rels.REPLY_OF_POST,
                        Rels.REPLY_OF_COMMENT ).iterator() )
                {
                    if ( !replyOfs.hasNext() )
                    {
                        // Original message: (message:Post)
                        originalPostMap.put( message, message );
                        return message;
                    }
                    else
                    {
                        Relationship replyOf = replyOfs.next();
                        if ( replyOf.isType( Rels.REPLY_OF_POST ) )
                        {
                            // Original message: (message:Comment)-->(Post)
                            post = replyOf.getEndNode();
                            originalPostMap.put( message, post );
                            return post;
                        }
                        else
                        {
                            // Original message: (message:Comment)-->(Comment)
                            List<Node> intermediaryComments = new ArrayList<>();
                            intermediaryComments.add( message );
                            post = innerGetPost( replyOf.getEndNode(), intermediaryComments );
                            for ( Node comment : intermediaryComments )
                            {
                                originalPostMap.put( comment, post );
                            }
                            return post;
                        }
                    }
                }
            }
        }

        private Node innerGetPost( Node message, List<Node> intermediaryComments )
        {
            Node post = originalPostMap.get( message );
            if ( null != post )
            {
                return post;
            }
            else
            {
                intermediaryComments.add( message );

                try ( ResourceIterator<Relationship> replyOfs = (ResourceIterator<Relationship>) message.getRelationships(
                        Direction.OUTGOING,
                        Rels.REPLY_OF_POST,
                        Rels.REPLY_OF_COMMENT ).iterator() )
                {
                    if ( !replyOfs.hasNext() )
                    {
                        // Original message: (message:Post)
                        return message;
                    }
                    else
                    {
                        intermediaryComments.add( message );
                        Relationship replyOf = replyOfs.next();
                        if ( replyOf.isType( Rels.REPLY_OF_POST ) )
                        {
                            // Original message: (message:Comment)-->(Post)
                            return replyOf.getEndNode();
                        }
                        else
                        {
                            // Original message: (message:Comment)-->(Comment)
                            return innerGetPost( replyOf.getEndNode(), intermediaryComments );
                        }
                    }
                }
            }
        }
    }

    private static class MessageDetails
    {
        private final Node message;
        private final long messageCreationDate;
        private final boolean isPost;
        private long id;
        private String content;

        private MessageDetails( Node message, long messageCreationDate, boolean isPost )
        {
            this.message = message;
            this.messageCreationDate = messageCreationDate;
            this.isPost = isPost;
            this.id = -1;
            this.content = null;
        }

        public long id()
        {
            if ( -1 == id )
            {
                id = (long) message.getProperty( Message.ID );
            }
            return id;
        }

        public String content()
        {
            if ( null == content )
            {
                content = isPost
                          ? (message.hasProperty( Message.CONTENT ))
                            ? (String) message.getProperty( Message.CONTENT )
                            : (String) message.getProperty( Post.IMAGE_FILE )
                          : (String) message.getProperty( Message.CONTENT );
            }
            return content;
        }

        public long creationDate()
        {
            return messageCreationDate;
        }

        public Node message()
        {
            return message;
        }
    }

    private static class DescendingCreationDateAscendingIdComparator implements Comparator<MessageDetails>
    {
        @Override
        public int compare( MessageDetails messageDetails1, MessageDetails messageDetails2 )
        {
            if ( messageDetails1.creationDate() > messageDetails2.creationDate() )
            {
                return -1;
            }
            else if ( messageDetails1.creationDate() < messageDetails2.creationDate() )
            {
                return 1;
            }
            else
            {
                if ( messageDetails1.id() < messageDetails2.id() )
                {
                    return -1;
                }
                else if ( messageDetails1.id() > messageDetails2.id() )
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }
        }
    }
}
