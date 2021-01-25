/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10TagPerson;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery11UnrelatedReplies;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery12TrendingPosts;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13PopularMonthlyTags;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14TopThreadInitiators;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery15SocialNormals;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircle;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17FriendshipTriangles;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery18PersonPostCounts;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19StrangerInteraction;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1PostingSummary;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery20HighLevelTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery21Zombies;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22InternationalDialog;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23HolidayDestinations;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery24MessagesByTopic;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery25WeightedPaths;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTags;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery3TagEvolution;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery4PopularCountryTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery5TopCountryPosters;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery6ActivePosters;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery7AuthoritativeUsers;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery8RelatedTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery9RelatedForums;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery1;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery10;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery11;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery12;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery13;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery14;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery15;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery16;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery17;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery18;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery19;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery2;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery20;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery21;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery22;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery23;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery24;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery25;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery3;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery4;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery5;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery6;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery7;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery8;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery9;
import com.neo4j.bench.ldbc.utils.AnnotatedQueries;
import com.neo4j.bench.ldbc.utils.AnnotatedQuery;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import static java.lang.String.format;

public class SnbBiCypherQueries implements AnnotatedQueries
{
    public static SnbBiCypherQueries none()
    {
        return new SnbBiCypherQueries();
    }

    public static SnbBiCypherQueries createWith( Planner plannerType, Runtime runtimeType )
    {
        return new SnbBiCypherQueries()
                .withQuery(
                        LdbcSnbBiQuery1PostingSummary.TYPE,
                        Neo4jSnbBiQuery1.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery2TopTags.TYPE,
                        Neo4jSnbBiQuery2.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery3TagEvolution.TYPE,
                        Neo4jSnbBiQuery3.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery4PopularCountryTopics.TYPE,
                        Neo4jSnbBiQuery4.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery5TopCountryPosters.TYPE,
                        Neo4jSnbBiQuery5.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery6ActivePosters.TYPE,
                        Neo4jSnbBiQuery6.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery7AuthoritativeUsers.TYPE,
                        Neo4jSnbBiQuery7.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery8RelatedTopics.TYPE,
                        Neo4jSnbBiQuery8.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery9RelatedForums.TYPE,
                        Neo4jSnbBiQuery9.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery10TagPerson.TYPE,
                        Neo4jSnbBiQuery10.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery11UnrelatedReplies.TYPE,
                        Neo4jSnbBiQuery11.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery12TrendingPosts.TYPE,
                        Neo4jSnbBiQuery12.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery13PopularMonthlyTags.TYPE,
                        Neo4jSnbBiQuery13.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery14TopThreadInitiators.TYPE,
                        Neo4jSnbBiQuery14.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery15SocialNormals.TYPE,
                        Neo4jSnbBiQuery15.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery16ExpertsInSocialCircle.TYPE,
                        Neo4jSnbBiQuery16.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery17FriendshipTriangles.TYPE,
                        Neo4jSnbBiQuery17.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery18PersonPostCounts.TYPE,
                        Neo4jSnbBiQuery18.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery19StrangerInteraction.TYPE,
                        Neo4jSnbBiQuery19.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery20HighLevelTopics.TYPE,
                        Neo4jSnbBiQuery20.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery21Zombies.TYPE,
                        Neo4jSnbBiQuery21.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery22InternationalDialog.TYPE,
                        Neo4jSnbBiQuery22.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery23HolidayDestinations.TYPE,
                        Neo4jSnbBiQuery23.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery24MessagesByTopic.TYPE,
                        Neo4jSnbBiQuery24.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery25WeightedPaths.TYPE,
                        Neo4jSnbBiQuery25.QUERY_STRING,
                        runtimeType,
                        plannerType );
    }

    private final Int2ObjectMap<AnnotatedQuery> annotatedQueries;

    private SnbBiCypherQueries()
    {
        this.annotatedQueries = new Int2ObjectOpenHashMap<>();
    }

    private SnbBiCypherQueries withQuery(
            int operationType,
            String queryString,
            Runtime runtimeType,
            Planner plannerType )
    {
        annotatedQueries.put(
                operationType,
                new AnnotatedQuery(
                        queryString,
                        plannerType,
                        runtimeType
                )
        );
        return this;
    }

    @Override
    public AnnotatedQuery queryFor( Operation operation ) throws DbException
    {
        if ( !annotatedQueries.containsKey( operation.type() ) )
        {
            throw new DbException( format( "Operation type %s has no associated query", operation.type() ) );
        }
        return annotatedQueries.get( operation.type() );
    }
}
