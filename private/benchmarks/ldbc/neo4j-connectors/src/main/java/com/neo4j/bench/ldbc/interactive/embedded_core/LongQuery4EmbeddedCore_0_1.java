/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.Domain.Tag;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery4;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class LongQuery4EmbeddedCore_0_1 extends Neo4jQuery4<Neo4jConnectionState>
{
    private static final DescendingPostCountAscendingTagNameComparator
            DESCENDING_POST_COUNT_ASCENDING_TAG_NAME_COMPARATOR =
            new DescendingPostCountAscendingTagNameComparator();

    @Override
    public List<LdbcQuery4Result> execute( Neo4jConnectionState connection, final LdbcQuery4 operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        long startDate = dateUtil.utcToFormat(
                operation.startDate().getTime() );
        long endDate = dateUtil.utcToFormat(
                operation.startDate().getTime() + TimeUnit.DAYS.toMillis( operation.durationDays() ) );
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );

        Set<Node> tagsOnOldPosts = new HashSet<>();

        HashMap<Node,Integer> tagPostCounts = new HashMap<>();
        for ( Relationship knows : person.getRelationships( Rels.KNOWS ) )
        {
            Node friend = knows.getOtherNode( person );
            for ( Relationship hasCreator : friend.getRelationships( Direction.INCOMING, Rels.POST_HAS_CREATOR ) )
            {
                Node post = hasCreator.getStartNode();
                long postCreationDate = (long) post.getProperty( Message.CREATION_DATE );
                if ( postCreationDate < startDate )
                {
                    tagsOnOldPosts.addAll( tagsOnPost( post ) );
                }
                else if ( postCreationDate < endDate )
                {
                    for ( Relationship hasTag : post.getRelationships( Direction.OUTGOING, Rels.POST_HAS_TAG ) )
                    {
                        Node tag = hasTag.getEndNode();
                        Integer postCount = tagPostCounts.get( tag );
                        if ( null == postCount )
                        {
                            tagPostCounts.put( tag, 1 );
                        }
                        else
                        {
                            tagPostCounts.put( tag, postCount + 1 );
                        }
                    }
                }
            }
        }

        List<LdbcQuery4PreResult> preResults = new ArrayList<>();
        for ( Map.Entry<Node,Integer> tagPostCount : tagPostCounts.entrySet() )
        {
            Node tag = tagPostCount.getKey();
            if ( !tagsOnOldPosts.contains( tag ) )
            {
                int postCount = tagPostCount.getValue();
                preResults.add( new LdbcQuery4PreResult( tag, postCount ) );
            }
        }

        Collections.sort( preResults, DESCENDING_POST_COUNT_ASCENDING_TAG_NAME_COMPARATOR );

        List<LdbcQuery4Result> results = new ArrayList<>();
        for ( int i = 0; i < preResults.size() && i < operation.limit(); i++ )
        {
            LdbcQuery4PreResult preResult = preResults.get( i );
            results.add(
                    new LdbcQuery4Result(
                            preResult.tagName(),
                            preResult.postCount()
                    )
            );
        }

        return results;
    }

    private List<Node> tagsOnPost( Node post )
    {
        List<Node> tags = new ArrayList<>();
        for ( Relationship hasTag : post.getRelationships( Direction.OUTGOING, Rels.POST_HAS_TAG ) )
        {
            Node tag = hasTag.getEndNode();
            tags.add( tag );
        }
        return tags;
    }

    private static class LdbcQuery4PreResult
    {
        private final Node tag;
        private final int postCount;
        private String tagName;

        private LdbcQuery4PreResult( Node tag, int postCount )
        {
            this.tag = tag;
            this.postCount = postCount;
        }

        private String tagName()
        {
            if ( null == tagName )
            {
                tagName = (String) tag.getProperty( Tag.NAME );
            }
            return tagName;
        }

        private int postCount()
        {
            return postCount;
        }
    }

    private static class DescendingPostCountAscendingTagNameComparator implements Comparator<LdbcQuery4PreResult>
    {
        @Override
        public int compare( LdbcQuery4PreResult result1, LdbcQuery4PreResult result2 )
        {
            if ( result1.postCount() > result2.postCount() )
            {
                return -1;
            }
            else if ( result1.postCount() < result2.postCount() )
            {
                return 1;
            }
            else
            {
                return result1.tagName().compareTo( result2.tagName() );
            }
        }
    }
}
