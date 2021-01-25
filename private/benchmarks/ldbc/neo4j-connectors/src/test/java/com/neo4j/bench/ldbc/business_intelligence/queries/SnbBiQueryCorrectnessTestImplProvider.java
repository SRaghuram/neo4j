/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries;

import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10TagPerson;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10TagPersonResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery11UnrelatedReplies;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery11UnrelatedRepliesResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery12TrendingPosts;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery12TrendingPostsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13PopularMonthlyTags;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13PopularMonthlyTagsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14TopThreadInitiators;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14TopThreadInitiatorsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery15SocialNormals;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery15SocialNormalsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircle;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircleResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17FriendshipTriangles;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17FriendshipTrianglesResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery18PersonPostCounts;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery18PersonPostCountsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19StrangerInteraction;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19StrangerInteractionResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1PostingSummary;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1PostingSummaryResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery20HighLevelTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery20HighLevelTopicsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery21Zombies;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery21ZombiesResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22InternationalDialog;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22InternationalDialogResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23HolidayDestinations;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23HolidayDestinationsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery24MessagesByTopic;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery24MessagesByTopicResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTags;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTagsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery3TagEvolution;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery3TagEvolutionResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery4PopularCountryTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery4PopularCountryTopicsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery5TopCountryPosters;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery5TopCountryPostersResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery6ActivePosters;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery6ActivePostersResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery7AuthoritativeUsers;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery7AuthoritativeUsersResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery8RelatedTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery8RelatedTopicsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery9RelatedForums;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery9RelatedForumsResult;

import java.util.List;

public interface SnbBiQueryCorrectnessTestImplProvider<CONNECTION>
{
    CONNECTION openConnection( String path ) throws Exception;

    void closeConnection( CONNECTION connection ) throws Exception;

    List<LdbcSnbBiQuery1PostingSummaryResult> neo4jQuery1Impl(
            CONNECTION connection,
            LdbcSnbBiQuery1PostingSummary operation ) throws Exception;

    List<LdbcSnbBiQuery2TopTagsResult> neo4jQuery2Impl(
            CONNECTION connection,
            LdbcSnbBiQuery2TopTags operation ) throws Exception;

    List<LdbcSnbBiQuery3TagEvolutionResult> neo4jQuery3Impl(
            CONNECTION connection,
            LdbcSnbBiQuery3TagEvolution operation ) throws Exception;

    List<LdbcSnbBiQuery4PopularCountryTopicsResult> neo4jQuery4Impl(
            CONNECTION connection,
            LdbcSnbBiQuery4PopularCountryTopics operation ) throws Exception;

    List<LdbcSnbBiQuery5TopCountryPostersResult> neo4jQuery5Impl(
            CONNECTION connection,
            LdbcSnbBiQuery5TopCountryPosters operation ) throws Exception;

    List<LdbcSnbBiQuery6ActivePostersResult> neo4jQuery6Impl(
            CONNECTION connection,
            LdbcSnbBiQuery6ActivePosters operation ) throws Exception;

    List<LdbcSnbBiQuery7AuthoritativeUsersResult> neo4jQuery7Impl(
            CONNECTION connection,
            LdbcSnbBiQuery7AuthoritativeUsers operation ) throws Exception;

    List<LdbcSnbBiQuery8RelatedTopicsResult> neo4jQuery8Impl(
            CONNECTION connection,
            LdbcSnbBiQuery8RelatedTopics operation ) throws Exception;

    List<LdbcSnbBiQuery9RelatedForumsResult> neo4jQuery9Impl(
            CONNECTION connection,
            LdbcSnbBiQuery9RelatedForums operation ) throws Exception;

    List<LdbcSnbBiQuery10TagPersonResult> neo4jQuery10Impl(
            CONNECTION connection,
            LdbcSnbBiQuery10TagPerson operation ) throws Exception;

    List<LdbcSnbBiQuery11UnrelatedRepliesResult> neo4jQuery11Impl(
            CONNECTION connection,
            LdbcSnbBiQuery11UnrelatedReplies operation ) throws Exception;

    List<LdbcSnbBiQuery12TrendingPostsResult> neo4jQuery12Impl(
            CONNECTION connection,
            LdbcSnbBiQuery12TrendingPosts operation ) throws Exception;

    List<LdbcSnbBiQuery13PopularMonthlyTagsResult> neo4jQuery13Impl(
            CONNECTION connection,
            LdbcSnbBiQuery13PopularMonthlyTags operation ) throws Exception;

    List<LdbcSnbBiQuery14TopThreadInitiatorsResult> neo4jQuery14Impl(
            CONNECTION connection,
            LdbcSnbBiQuery14TopThreadInitiators operation ) throws Exception;

    List<LdbcSnbBiQuery15SocialNormalsResult> neo4jQuery15Impl(
            CONNECTION connection,
            LdbcSnbBiQuery15SocialNormals operation ) throws Exception;

    List<LdbcSnbBiQuery16ExpertsInSocialCircleResult> neo4jQuery16Impl(
            CONNECTION connection,
            LdbcSnbBiQuery16ExpertsInSocialCircle operation ) throws Exception;

    LdbcSnbBiQuery17FriendshipTrianglesResult neo4jQuery17Impl(
            CONNECTION connection,
            LdbcSnbBiQuery17FriendshipTriangles operation ) throws Exception;

    List<LdbcSnbBiQuery18PersonPostCountsResult> neo4jQuery18Impl(
            CONNECTION connection,
            LdbcSnbBiQuery18PersonPostCounts operation ) throws Exception;

    List<LdbcSnbBiQuery19StrangerInteractionResult> neo4jQuery19Impl(
            CONNECTION connection,
            LdbcSnbBiQuery19StrangerInteraction operation ) throws Exception;

    List<LdbcSnbBiQuery20HighLevelTopicsResult> neo4jQuery20Impl(
            CONNECTION connection,
            LdbcSnbBiQuery20HighLevelTopics operation ) throws Exception;

    List<LdbcSnbBiQuery21ZombiesResult> neo4jQuery21Impl(
            CONNECTION connection,
            LdbcSnbBiQuery21Zombies operation ) throws Exception;

    List<LdbcSnbBiQuery22InternationalDialogResult> neo4jQuery22Impl(
            CONNECTION connection,
            LdbcSnbBiQuery22InternationalDialog operation ) throws Exception;

    List<LdbcSnbBiQuery23HolidayDestinationsResult> neo4jQuery23Impl(
            CONNECTION connection,
            LdbcSnbBiQuery23HolidayDestinations operation ) throws Exception;

    List<LdbcSnbBiQuery24MessagesByTopicResult> neo4jQuery24Impl(
            CONNECTION connection,
            LdbcSnbBiQuery24MessagesByTopic operation ) throws Exception;

}
