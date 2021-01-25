/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.neo4j.bench.ldbc.Domain.Forum;
import com.neo4j.bench.ldbc.Domain.HasMember;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Place;
import com.neo4j.bench.ldbc.Domain.Post;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.Domain.Tag;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.connection.TimeStampedRelationshipTypesCache;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate6;
import com.neo4j.bench.ldbc.operators.Operators;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;

import static java.lang.String.format;

public class Update6EmbeddedCore_1 extends Neo4jUpdate6<Neo4jConnectionState>
{
    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate6AddPost operation ) throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node post = connection.getTx().createNode( Nodes.Post, Nodes.Message );
        post.setProperty( Message.ID, operation.postId() );
        if ( !operation.imageFile().isEmpty() )
        {
            post.setProperty( Post.IMAGE_FILE, operation.imageFile() );
        }
        long creationDate = dateUtil.utcToFormat( operation.creationDate().getTime() );
        post.setProperty( Message.CREATION_DATE, creationDate );
        post.setProperty( Message.LOCATION_IP, operation.locationIp() );
        post.setProperty( Message.BROWSER_USED, operation.browserUsed() );
        post.setProperty( Post.LANGUAGE, operation.language() );
        if ( !operation.content().isEmpty() )
        {
            post.setProperty( Message.CONTENT, operation.content() );
        }
        post.setProperty( Message.LENGTH, operation.length() );

        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.authorPersonId() );

        post.createRelationshipTo( person, Rels.POST_HAS_CREATOR );

        TimeStampedRelationshipTypesCache timeStampedRelationshipTypes = connection.timeStampedRelationshipTypesCache();

        long creationDateAtResolution = dateUtil.formatToEncodedDateAtResolution( creationDate );
        RelationshipType hasCreatorAtTime = timeStampedRelationshipTypes.postHasCreatorForDateAtResolution(
                creationDateAtResolution,
                dateUtil );
        post.createRelationshipTo( person, hasCreatorAtTime );

        Node forum = Operators.findNode( connection.getTx(), Nodes.Forum, Forum.ID, operation.forumId() );
        forum.createRelationshipTo( post, Rels.CONTAINER_OF );

        Node country = Operators.findNode( connection.getTx(), Place.Type.Country, Place.ID, operation.countryId() );
        RelationshipType isLocatedInAtTime = timeStampedRelationshipTypes.postIsLocatedInForDateAtResolution(
                creationDateAtResolution,
                dateUtil );
        post.createRelationshipTo( country, isLocatedInAtTime );

        for ( Long tagId : operation.tagIds() )
        {
            Node tag = Operators.findNode( connection.getTx(), Nodes.Tag, Tag.ID, tagId );
            post.createRelationshipTo( tag, Rels.POST_HAS_TAG );
        }

        if ( !isModerator( forum, person ) )
        {
            if ( !isMemberWithPosts( forum, person ) )
            {
                RelationshipType[] hasMemberRelationshipTypes = timeStampedRelationshipTypes.hasMemberForAllDates();
                Relationship hasMember = getHasMemberOrNull( forum, person, hasMemberRelationshipTypes );
                if ( null == hasMember )
                {
                    throw new DbException( format( "No %s relationship found between Forum %s & Person %s",
                            Rels.HAS_MEMBER.name(), operation.forumId(), operation.authorPersonId() ) );
                }
                else
                {
                    Relationship hasMemberWithPosts = forum.createRelationshipTo( person, Rels.HAS_MEMBER_WITH_POSTS );
                    long joinDate = (long) hasMember.getProperty( HasMember.JOIN_DATE );
                    hasMemberWithPosts.setProperty( HasMember.JOIN_DATE, joinDate );
                }
            }
        }

        return LdbcNoResult.INSTANCE;
    }

    private boolean isModerator( Node desiredForum, Node desiredPerson )
    {
        return desiredPerson.equals(
                desiredForum.getSingleRelationship( Rels.HAS_MODERATOR, Direction.OUTGOING ).getEndNode()
        );
    }

    private boolean isMemberWithPosts( Node desiredForum, Node desiredPerson )
    {
        try ( ResourceIterator<Relationship> hasMemberWithPostsIter =
                      (ResourceIterator<Relationship>) desiredForum.getRelationships( Direction.OUTGOING, Rels.HAS_MEMBER_WITH_POSTS ).iterator() )
        {
            while ( hasMemberWithPostsIter.hasNext() )
            {
                Relationship hasMemberWithPosts = hasMemberWithPostsIter.next();
                Node person = hasMemberWithPosts.getEndNode();
                if ( person.equals( desiredPerson ) )
                {
                    return true;
                }
            }
        }
        return false;
    }

    private Relationship getHasMemberOrNull(
            Node desiredForum,
            Node desiredPerson,
            RelationshipType[] hasMemberRelationshipTypes )
    {
        try ( ResourceIterator<Relationship> hasMembers =
                      (ResourceIterator<Relationship>) desiredForum.getRelationships( Direction.OUTGOING, hasMemberRelationshipTypes ).iterator() )
        {
            while ( hasMembers.hasNext() )
            {
                Relationship hasMember = hasMembers.next();
                Node person = hasMember.getEndNode();
                if ( person.equals( desiredPerson ) )
                {
                    return hasMember;
                }
            }
        }
        return null;
    }
}
