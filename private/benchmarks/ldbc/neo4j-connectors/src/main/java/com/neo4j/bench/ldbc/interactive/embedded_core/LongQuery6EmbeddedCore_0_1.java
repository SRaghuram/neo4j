/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.google.common.collect.MinMaxPriorityQueue;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.Domain.Tag;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery6;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class LongQuery6EmbeddedCore_0_1 extends Neo4jQuery6<Neo4jConnectionState>
{
    private static final DescendingPostCountThenAscendingTagNameComparator
            DESCENDING_POST_COUNT_THEN_ASCENDING_TAG_NAME_COMPARATOR =
            new DescendingPostCountThenAscendingTagNameComparator();

    @Override
    public List<LdbcQuery6Result> execute( Neo4jConnectionState connection, LdbcQuery6 operation )
            throws DbException
    {
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );
        Node knownTag = Operators.findNode( connection.getTx(), Nodes.Tag, Tag.NAME, operation.tagName() );

        Set<Node> friends = new HashSet<>();
        for ( Relationship knows : person.getRelationships( Rels.KNOWS ) )
        {
            Node friend = knows.getOtherNode( person );
            friends.add( friend );
            for ( Relationship knowsKnows : friend.getRelationships( Rels.KNOWS ) )
            {
                Node friendsFriend = knowsKnows.getOtherNode( friend );
                if ( !friendsFriend.equals( person ) )
                {
                    friends.add( friendsFriend );
                }
            }
        }

        Map<Node,Integer> tagPostCounts = new HashMap<>();
        for ( Relationship hasKnownTag : knownTag.getRelationships( Direction.INCOMING, Rels.POST_HAS_TAG ) )
        {
            Node post = hasKnownTag.getStartNode();
            Node creator = post.getSingleRelationship( Rels.POST_HAS_CREATOR, Direction.OUTGOING ).getEndNode();
            if ( friends.contains( creator ) )
            {
                for ( Relationship hasTag : post.getRelationships( Direction.OUTGOING, Rels.POST_HAS_TAG ) )
                {
                    Node tag = hasTag.getEndNode();
                    if ( !tag.equals( knownTag ) )
                    {
                        Integer tagPostCount = tagPostCounts.get( tag );
                        if ( null == tagPostCount )
                        {
                            tagPostCounts.put( tag, 1 );
                        }
                        else
                        {
                            tagPostCounts.put( tag, tagPostCount + 1 );
                        }
                    }
                }
            }
        }

        MinMaxPriorityQueue<LdbcQuery6PreResult> preResults = MinMaxPriorityQueue
                .orderedBy( DESCENDING_POST_COUNT_THEN_ASCENDING_TAG_NAME_COMPARATOR )
                .maximumSize( operation.limit() )
                .create();

        for ( Entry<Node,Integer> tagPostCount : tagPostCounts.entrySet() )
        {
            preResults.add(
                    new LdbcQuery6PreResult( tagPostCount.getKey(), tagPostCount.getValue() )
            );
        }

        List<LdbcQuery6Result> results = new ArrayList<>();
        LdbcQuery6PreResult preResult;
        while ( null != (preResult = preResults.poll()) )
        {
            results.add(
                    new LdbcQuery6Result( preResult.tagName(), preResult.count() )
            );
        }

        return results;
    }

    static class DescendingPostCountThenAscendingTagNameComparator implements Comparator<LdbcQuery6PreResult>
    {
        @Override
        public int compare( LdbcQuery6PreResult ldbcQuery6PreResult1, LdbcQuery6PreResult ldbcQuery6PreResult2 )
        {
            if ( ldbcQuery6PreResult1.count() > ldbcQuery6PreResult2.count() )
            {
                return -1;
            }
            else if ( ldbcQuery6PreResult1.count() < ldbcQuery6PreResult2.count() )
            {
                return 1;
            }
            else
            {
                return ldbcQuery6PreResult1.tagName().compareTo( ldbcQuery6PreResult2.tagName() );
            }
        }
    }

    private static class LdbcQuery6PreResult
    {
        private final Node tag;
        private final int count;
        private String tagName;

        private LdbcQuery6PreResult( Node tag, int count )
        {
            this.tag = tag;
            this.count = count;
        }

        private String tagName()
        {
            if ( null == tagName )
            {
                tagName = (String) tag.getProperty( Tag.NAME );
            }
            return tagName;
        }

        private int count()
        {
            return count;
        }
    }
}
