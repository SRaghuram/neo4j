/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.google.common.base.Predicate;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.Domain.Tag;
import com.neo4j.bench.ldbc.Domain.TagClass;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery12;
import com.neo4j.bench.ldbc.operators.ManyToManyExpandCache;
import com.neo4j.bench.ldbc.operators.NodePropertyValueCache;
import com.neo4j.bench.ldbc.operators.OneToManyIsConnectedCache;
import com.neo4j.bench.ldbc.operators.Operators;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class LongQuery12EmbeddedCore_0_1 extends Neo4jQuery12<Neo4jConnectionState>
{
    private static final DescendingCommentCountAscendingPersonIdentifier
            DESCENDING_COMMENT_COUNT_ASCENDING_PERSON_IDENTIFIER =
            new DescendingCommentCountAscendingPersonIdentifier();

    @Override
    public List<LdbcQuery12Result> execute( Neo4jConnectionState connection, LdbcQuery12 operation ) throws DbException
    {
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );
        Node tagClass = Operators.findNode( connection.getTx(), Nodes.TagClass, TagClass.NAME, operation.tagClassName() );

        Map<Node,LdbcQuery12PreResult> preResults = new HashMap<>();
        for ( Relationship knows : person.getRelationships( Rels.KNOWS ) )
        {
            Node friend = knows.getOtherNode( person );
            int commentCount = 0;
            preResults.put( friend, new LdbcQuery12PreResult( friend, commentCount ) );
        }

        LongSet validTagClasses = Operators.expandIdsTransitive(
                tagClass,
                Direction.INCOMING,
                Rels.IS_SUBCLASS_OF );
        validTagClasses.add( tagClass.getId() );
        final OneToManyIsConnectedCache validTagsCache = Operators
                .oneToManyIsConnected()
                .lazyPull( validTagClasses, Direction.OUTGOING, Rels.HAS_TYPE );
        Predicate<Node> validTagPredicate = new Predicate<>()
        {
            @Override
            public boolean apply( Node tag )
            {
                try
                {
                    return validTagsCache.isConnected( tag );
                }
                catch ( DbException e )
                {
                    throw new RuntimeException( "Neighbor predicate error", e );
                }
            }
        };
        ManyToManyExpandCache postTagsCache = Operators.manyToManyExpand().lazyPullAnyPredicate(
                validTagPredicate,
                Direction.OUTGOING,
                Rels.POST_HAS_TAG );
        TagsOnPostCache tagsOnPost = new TagsOnPostCache( postTagsCache );

        for ( Map.Entry<Node,LdbcQuery12PreResult> friendEntry : preResults.entrySet() )
        {
            Node friend = friendEntry.getKey();
            LdbcQuery12PreResult preResult = friendEntry.getValue();
            for ( Relationship hasCreator : friend.getRelationships( Direction.INCOMING, Rels.COMMENT_HAS_CREATOR ) )
            {
                Node comment = hasCreator.getStartNode();
                LongSet tagsOnRepliedToPost = tagsOnPost.getTags( comment );
                if ( !tagsOnRepliedToPost.isEmpty() )
                {
                    // reply is connected to a post with at least one valid tag
                    preResult.setCommentCount( preResult.commentCount() + 1 );
                    preResult.addTags( tagsOnRepliedToPost );
                }
            }
        }

        List<LdbcQuery12PreResult> preResultsList = new ArrayList<>( preResults.size() );
        for ( LdbcQuery12PreResult preResult : preResults.values() )
        {
            if ( preResult.commentCount() > 0 )
            {
                preResultsList.add( preResult );
            }
        }

        Collections.sort( preResultsList, DESCENDING_COMMENT_COUNT_ASCENDING_PERSON_IDENTIFIER );

        NodePropertyValueCache<String> tagNameCache = Operators.propertyValue().create( Tag.NAME );
        List<LdbcQuery12Result> results = new ArrayList<>( operation.limit() );
        for ( int i = 0; i < preResultsList.size() && i < operation.limit(); i++ )
        {
            LdbcQuery12PreResult preResult = preResultsList.get( i );
            Map<String,Object> friendProperties = preResult.friendProperties();
            results.add(
                    new LdbcQuery12Result(
                            (long) friendProperties.get( Person.ID ),
                            (String) friendProperties.get( Person.FIRST_NAME ),
                            (String) friendProperties.get( Person.LAST_NAME ),
                            preResult.tags( tagNameCache, connection.getTx() ),
                            preResult.commentCount()
                    )
            );
        }

        return results;
    }

    private static class TagsOnPostCache
    {
        private static final LongSet EMPTY_SET = new LongOpenHashSet();
        private final ManyToManyExpandCache postTagsCache;

        private TagsOnPostCache( ManyToManyExpandCache postTagsCache ) throws DbException
        {
            this.postTagsCache = postTagsCache;
        }

        // retrieves the tags on the replied-to post iff post has at least one valid tag
        private LongSet getTags( Node comment ) throws DbException
        {
            Relationship replyOfPost = comment.getSingleRelationship( Rels.REPLY_OF_POST, Direction.OUTGOING );
            if ( null == replyOfPost )
            {
                // Only consider comments that directly reply to posts
                return EMPTY_SET;
            }
            Node post = replyOfPost.getEndNode();
            // NOTE, this cache returns null when no tags are valid (in tag class)
            return postTagsCache.expandIds( post );
        }
    }

    public static class DescendingCommentCountAscendingPersonIdentifier implements Comparator<LdbcQuery12PreResult>
    {
        @Override
        public int compare( LdbcQuery12PreResult preResult1, LdbcQuery12PreResult preResult2 )
        {
            if ( preResult1.commentCount() > preResult2.commentCount() )
            {
                return -1;
            }
            else if ( preResult1.commentCount() < preResult2.commentCount() )
            {
                return 1;
            }
            else
            {
                if ( preResult1.friendId() < preResult2.friendId() )
                {
                    return -1;
                }
                else if ( preResult1.friendId() > preResult2.friendId() )
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

    private class LdbcQuery12PreResult
    {
        private final Node friend;
        private final LongSet tagIds;
        private long friendId;
        private int commentCount;

        private LdbcQuery12PreResult( Node friend, int commentCount )
        {
            this.friend = friend;
            this.friendId = -1;
            this.tagIds = new LongOpenHashSet();
            this.commentCount = commentCount;
        }

        private long friendId()
        {
            if ( -1 == friendId )
            {
                friendId = (long) friend.getProperty( Person.ID );
            }
            return friendId;
        }

        private Map<String,Object> friendProperties()
        {
            if ( -1 == friendId )
            {
                return friend.getProperties( Person.ID, Person.FIRST_NAME, Person.LAST_NAME );
            }
            else
            {
                Map<String,Object> personProperties = friend.getProperties( Person.FIRST_NAME, Person.LAST_NAME );
                personProperties.put( Person.ID, friendId );
                return personProperties;
            }
        }

        private int commentCount()
        {
            return commentCount;
        }

        private void setCommentCount( int newCommentCount )
        {
            this.commentCount = newCommentCount;
        }

        private List<String> tags( NodePropertyValueCache<String> tagNameCache, Transaction tx )
        {
            List<String> tags = new ArrayList<>( tagIds.size() );
            for ( Long tagId : tagIds )
            {
                tags.add( tagNameCache.value( tagId, tx ) );
            }
            return tags;
        }

        private void addTags( LongSet moreTags )
        {
            tagIds.addAll( moreTags );
        }
    }
}
