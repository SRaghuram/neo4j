/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.neo4j.bench.ldbc.Domain.Knows;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Place;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.Domain.Tag;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery14;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate7;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.HashMap;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;

public class Update7EmbeddedCore_1 extends Neo4jUpdate7<Neo4jConnectionState>
{
    private static final RelationshipType[] POST_HAS_CREATOR_RELATIONSHIP_TYPES =
            new RelationshipType[]{Rels.POST_HAS_CREATOR};
    private static final RelationshipType[] COMMENT_HAS_CREATOR_RELATIONSHIP_TYPES =
            new RelationshipType[]{Rels.COMMENT_HAS_CREATOR};

    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate7AddComment operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node comment = connection.getTx().createNode( Nodes.Comment, Nodes.Message );
        comment.setProperty( Message.ID, operation.commentId() );
        long creationDate = dateUtil.utcToFormat( operation.creationDate().getTime() );
        comment.setProperty( Message.CREATION_DATE, creationDate );
        comment.setProperty( Message.LOCATION_IP, operation.locationIp() );
        comment.setProperty( Message.BROWSER_USED, operation.browserUsed() );
        comment.setProperty( Message.CONTENT, operation.content() );
        comment.setProperty( Message.LENGTH, operation.length() );

        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.authorPersonId() );

        comment.createRelationshipTo( person, Rels.COMMENT_HAS_CREATOR );

        RelationshipType hasCreatorAtTime =
                connection.timeStampedRelationshipTypesCache().commentHasCreatorForDateAtResolution(
                        dateUtil.formatToEncodedDateAtResolution( creationDate ),
                        dateUtil );
        comment.createRelationshipTo( person, hasCreatorAtTime );

        if ( -1 == operation.replyToPostId() )
        {
            Node replyToComment =
                    Operators.findNode( connection.getTx(), Nodes.Message, Message.ID, operation.replyToCommentId() );
            comment.createRelationshipTo( replyToComment, Rels.REPLY_OF_COMMENT );

            // maintain edge weights between persons - for query 14
            Node replyToCommentCreatorPerson =
                    replyToComment.getSingleRelationship( Rels.COMMENT_HAS_CREATOR, Direction.OUTGOING ).getEndNode();

            Relationship knows = getKnowsOrNull( person, replyToCommentCreatorPerson );
            if ( null != knows )
            {
                if ( knows.hasProperty( Knows.WEIGHT ) )
                {
                    double existingWeight = (double) knows.getProperty( Knows.WEIGHT );
                    knows.setProperty( Knows.WEIGHT, existingWeight + 0.5 );
                }
                else
                {
                    double existingWeight = Neo4jQuery14.conversationWeightBetweenPersons(
                            person,
                            replyToCommentCreatorPerson,
                            new HashMap<>(),
                            new HashMap<>(),
                            POST_HAS_CREATOR_RELATIONSHIP_TYPES,
                            COMMENT_HAS_CREATOR_RELATIONSHIP_TYPES );
                    knows.setProperty( Knows.WEIGHT, existingWeight );
                }
            }
        }
        else
        {
            Node replyToPost =
                    Operators.findNode( connection.getTx(), Nodes.Message, Message.ID, operation.replyToPostId() );
            comment.createRelationshipTo( replyToPost, Rels.REPLY_OF_POST );

            // maintain edge weights between persons - for query 14
            Node replyToPostCreatorPerson =
                    replyToPost.getSingleRelationship( Rels.POST_HAS_CREATOR, Direction.OUTGOING ).getEndNode();

            Relationship knows = getKnowsOrNull( person, replyToPostCreatorPerson );
            if ( null != knows )
            {
                if ( knows.hasProperty( Knows.WEIGHT ) )
                {
                    double existingWeight = (double) knows.getProperty( Knows.WEIGHT );
                    knows.setProperty( Knows.WEIGHT, existingWeight + 1.0 );
                }
                else
                {
                    double existingWeight = Neo4jQuery14.conversationWeightBetweenPersons(
                            person,
                            replyToPostCreatorPerson,
                            new HashMap<>(),
                            new HashMap<>(),
                            POST_HAS_CREATOR_RELATIONSHIP_TYPES,
                            COMMENT_HAS_CREATOR_RELATIONSHIP_TYPES );
                    knows.setProperty( Knows.WEIGHT, existingWeight );
                }
            }
        }

        Node country = Operators.findNode( connection.getTx(), Place.Type.Country, Place.ID, operation.countryId() );
        RelationshipType isLocatedInAtTime =
                connection.timeStampedRelationshipTypesCache().commentIsLocatedInForDateAtResolution(
                        dateUtil.formatToEncodedDateAtResolution( creationDate ),
                        dateUtil );
        comment.createRelationshipTo( country, isLocatedInAtTime );

        for ( Long tagId : operation.tagIds() )
        {
            Node tag = Operators.findNode( connection.getTx(), Nodes.Tag, Tag.ID, tagId );
            comment.createRelationshipTo( tag, Rels.COMMENT_HAS_TAG );
        }

        return LdbcNoResult.INSTANCE;
    }

    private Relationship getKnowsOrNull( Node person1, Node person2 )
    {
        try ( ResourceIterator<Relationship> knowsIter = (ResourceIterator<Relationship>) person1.getRelationships( Direction.BOTH, Rels.KNOWS ).iterator() )
        {
            while ( knowsIter.hasNext() )
            {
                Relationship knows = knowsIter.next();
                Node otherPerson = knows.getOtherNode( person1 );
                if ( otherPerson.equals( person2 ) )
                {
                    return knows;
                }
            }
        }
        return null;
    }
}
