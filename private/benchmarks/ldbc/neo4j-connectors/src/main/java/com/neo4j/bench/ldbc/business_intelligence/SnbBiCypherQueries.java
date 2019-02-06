/**
 * Copyright (c) 2002-2019 "Neo4j,"
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
import com.neo4j.bench.ldbc.utils.PlannerType;
import com.neo4j.bench.ldbc.utils.RuntimeType;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.Map;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class SnbBiCypherQueries implements AnnotatedQueries
{
    public static SnbBiCypherQueries none()
    {
        return new SnbBiCypherQueries();
    }

    public static SnbBiCypherQueries createWith( PlannerType plannerType, RuntimeType runtimeType )
    {
        return new SnbBiCypherQueries()
                .withQuery(
                        LdbcSnbBiQuery1PostingSummary.TYPE,
                        LdbcSnbBiQuery1PostingSummary.class,
                        Neo4jSnbBiQuery1.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery2TopTags.TYPE,
                        LdbcSnbBiQuery2TopTags.class,
                        Neo4jSnbBiQuery2.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery3TagEvolution.TYPE,
                        LdbcSnbBiQuery3TagEvolution.class,
                        Neo4jSnbBiQuery3.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery4PopularCountryTopics.TYPE,
                        LdbcSnbBiQuery4PopularCountryTopics.class,
                        Neo4jSnbBiQuery4.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery5TopCountryPosters.TYPE,
                        LdbcSnbBiQuery5TopCountryPosters.class,
                        Neo4jSnbBiQuery5.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery6ActivePosters.TYPE,
                        LdbcSnbBiQuery6ActivePosters.class,
                        Neo4jSnbBiQuery6.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery7AuthoritativeUsers.TYPE,
                        LdbcSnbBiQuery7AuthoritativeUsers.class,
                        Neo4jSnbBiQuery7.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery8RelatedTopics.TYPE,
                        LdbcSnbBiQuery8RelatedTopics.class,
                        Neo4jSnbBiQuery8.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery9RelatedForums.TYPE,
                        LdbcSnbBiQuery9RelatedForums.class,
                        Neo4jSnbBiQuery9.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery10TagPerson.TYPE,
                        LdbcSnbBiQuery10TagPerson.class,
                        Neo4jSnbBiQuery10.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery11UnrelatedReplies.TYPE,
                        LdbcSnbBiQuery11UnrelatedReplies.class,
                        Neo4jSnbBiQuery11.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery12TrendingPosts.TYPE,
                        LdbcSnbBiQuery12TrendingPosts.class,
                        Neo4jSnbBiQuery12.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery13PopularMonthlyTags.TYPE,
                        LdbcSnbBiQuery13PopularMonthlyTags.class,
                        Neo4jSnbBiQuery13.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery14TopThreadInitiators.TYPE,
                        LdbcSnbBiQuery14TopThreadInitiators.class,
                        Neo4jSnbBiQuery14.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery15SocialNormals.TYPE,
                        LdbcSnbBiQuery15SocialNormals.class,
                        Neo4jSnbBiQuery15.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery16ExpertsInSocialCircle.TYPE,
                        LdbcSnbBiQuery16ExpertsInSocialCircle.class,
                        Neo4jSnbBiQuery16.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery17FriendshipTriangles.TYPE,
                        LdbcSnbBiQuery17FriendshipTriangles.class,
                        Neo4jSnbBiQuery17.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery18PersonPostCounts.TYPE,
                        LdbcSnbBiQuery18PersonPostCounts.class,
                        Neo4jSnbBiQuery18.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery19StrangerInteraction.TYPE,
                        LdbcSnbBiQuery19StrangerInteraction.class,
                        Neo4jSnbBiQuery19.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery20HighLevelTopics.TYPE,
                        LdbcSnbBiQuery20HighLevelTopics.class,
                        Neo4jSnbBiQuery20.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery21Zombies.TYPE,
                        LdbcSnbBiQuery21Zombies.class,
                        Neo4jSnbBiQuery21.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery22InternationalDialog.TYPE,
                        LdbcSnbBiQuery22InternationalDialog.class,
                        Neo4jSnbBiQuery22.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery23HolidayDestinations.TYPE,
                        LdbcSnbBiQuery23HolidayDestinations.class,
                        Neo4jSnbBiQuery23.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery24MessagesByTopic.TYPE,
                        LdbcSnbBiQuery24MessagesByTopic.class,
                        Neo4jSnbBiQuery24.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcSnbBiQuery25WeightedPaths.TYPE,
                        LdbcSnbBiQuery25WeightedPaths.class,
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
            Class<? extends Operation> operationClass,
            String queryString,
            RuntimeType runtimeType,
            PlannerType plannerType )
    {
        annotatedQueries.put(
                operationType,
                new AnnotatedQuery(
                        operationType,
                        operationClass.getSimpleName(),
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

    @Override
    public Iterable<AnnotatedQuery> allQueries() throws DbException
    {
        return annotatedQueries.entrySet().stream().map( Map.Entry::getValue ).collect( toList() );
    }
}
