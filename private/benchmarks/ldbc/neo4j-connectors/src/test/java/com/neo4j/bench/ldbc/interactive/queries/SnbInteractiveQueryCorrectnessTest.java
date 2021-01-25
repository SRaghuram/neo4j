/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.queries;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;
import com.neo4j.bench.ldbc.QueryGraphMaker;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.ldbc.QueryGraphMaker.date;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@TestDirectoryExtension
public abstract class SnbInteractiveQueryCorrectnessTest<CONNECTION> implements
        SnbInteractiveQueryCorrectnessTestImplProvider<CONNECTION>
{
    @Inject
    public TestDirectory testFolder;

    @Test
    public void query1ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.Query1GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId = 0;
            String friendName = "name0";
            int limit = 6;
            LdbcQuery1 operation = new LdbcQuery1( personId, friendName, limit );

            Iterator<LdbcQuery1Result> results;
            LdbcQuery1Result actualRow;
            LdbcQuery1Result expectedRow;

            results = neo4jLongQuery1Impl( connection, operation ).iterator();

            actualRow = results.next();
            expectedRow = new LdbcQuery1Result(
                    2L,
                    "last0-\u16a0\u3055\u4e35\u05e4\u0634",
                    1,
                    2L,
                    2L,
                    "gender2",
                    "browser2",
                    "ip2",
                    Sets.<String>newHashSet(),
                    Sets.newHashSet( "friend2language0", "friend2language1" ),
                    "city1",
                    Sets.<List<Object>>newHashSet( Lists.<Object>newArrayList( "uni2", 3, "city0" ) ),
                    Sets.<List<Object>>newHashSet()
            );
            assertThat( actualRow, equalTo( expectedRow ) );

            actualRow = results.next();
            expectedRow = new LdbcQuery1Result(
                    3L,
                    "last0-\u16a0\u3055\u4e35\u05e4\u0634",
                    1,
                    3L,
                    3L,
                    "gender3",
                    "browser3",
                    "ip3",
                    Sets.newHashSet( "friend3email1", "friend3email2" ),
                    Sets.newHashSet( "friend3language0" ),
                    "city1",
                    Lists.<List<Object>>newArrayList(),
                    Lists.<List<Object>>newArrayList( Lists.<Object>newArrayList( "company0", 1, "country0" ) )
            );
            assertThat( actualRow, equalTo( expectedRow ) );

            actualRow = results.next();
            expectedRow = new LdbcQuery1Result(
                    1L,
                    "last1-\u16a0\u3055\u4e35\u05e4\u0634",
                    1,
                    1L,
                    1L,
                    "gender1",
                    "browser1",
                    "ip1",
                    Sets.newHashSet( "friend1email1", "friend1email2" ),
                    Sets.newHashSet( "friend1language0" ),
                    "city0",
                    Lists.<List<Object>>newArrayList( Lists.<Object>newArrayList( "uni0", 0, "city1" ) ),
                    Lists.<List<Object>>newArrayList( Lists.<Object>newArrayList( "company0", 0, "country0" ) )
            );
            assertThat( actualRow, equalTo( expectedRow ) );

            actualRow = results.next();
            expectedRow = new LdbcQuery1Result(
                    11L,
                    "last11-\u16a0\u3055\u4e35\u05e4\u0634",
                    2,
                    11L,
                    11L,
                    "gender11",
                    "browser11",
                    "ip11",
                    Sets.<String>newHashSet(),
                    Sets.<String>newHashSet(),
                    "city0",
                    Lists.<List<Object>>newArrayList( Lists.<Object>newArrayList( "uni1", 1, "city0" ),
                            Lists.<Object>newArrayList( "uni2", 2, "city0" ) ),
                    Lists.<List<Object>>newArrayList()
            );
            assertThat( actualRow, equalTo( expectedRow ) );

            actualRow = results.next();
            expectedRow = new LdbcQuery1Result(
                    31L,
                    "last31-\u16a0\u3055\u4e35\u05e4\u0634",
                    2,
                    31L,
                    31L,
                    "gender31",
                    "browser31",
                    "ip31",
                    Sets.<String>newHashSet(),
                    Sets.<String>newHashSet(),
                    "city1",
                    Lists.<List<Object>>newArrayList(),
                    Lists.<List<Object>>newArrayList()
            );
            assertThat( actualRow, equalTo( expectedRow ) );

            assertThat( results.hasNext(), is( false ) );

            personId = 0;
            friendName = "name1";
            limit = 1;
            operation = new LdbcQuery1( personId, friendName, limit );

            results = neo4jLongQuery1Impl( connection, operation ).iterator();

            actualRow = results.next();
            expectedRow = new LdbcQuery1Result(
                    21L,
                    "last21-\u16a0\u3055\u4e35\u05e4\u0634",
                    2,
                    21L,
                    21L,
                    "gender21",
                    "browser21",
                    "ip21",
                    Sets.<String>newHashSet(),
                    Sets.<String>newHashSet(),
                    "city1",
                    Lists.<List<Object>>newArrayList(),
                    Lists.<List<Object>>newArrayList( Lists.<Object>newArrayList( "company1", 2, "country1" ) )
            );
            assertThat( actualRow, equalTo( expectedRow ) );

            assertThat( results.hasNext(), is( false ) );

            /*
            ----- Apply updates to add person and friendship -----
             */

            // Add person

            long newPersonId = 4;
            String newPersonFirstName = "name0";
            String newPersonLastName = "last4-\u16a0\u3055\u4e35\u05e4\u0634";
            String newPersonGender = "gender4";
            long newPersonBirthday = 4L;
            long newPersonCreationDate = 4L;
            String newPersonLocationIp = "ip4";
            String newPersonBrowser = "browser4";
            long newPersonCityId = 0;
            List<String> newPersonLanguages = Lists.newArrayList( "friend4language0" );
            List<String> newPersonEmails = Lists.newArrayList( "friend4email1", "friend4email2" );
            List<Long> newPersonTags = new ArrayList<>();
            List<LdbcUpdate1AddPerson.Organization> newPersonUniversities = Lists.newArrayList(
                    new LdbcUpdate1AddPerson.Organization( 1L, 4 )
            );
            List<LdbcUpdate1AddPerson.Organization> newPersonCompanies = Lists.newArrayList(
                    new LdbcUpdate1AddPerson.Organization( 10L, 4 )
            );
            LdbcUpdate1AddPerson addPerson = new LdbcUpdate1AddPerson(
                    newPersonId,
                    newPersonFirstName,
                    newPersonLastName,
                    newPersonGender,
                    new Date( newPersonBirthday ),
                    new Date( newPersonCreationDate ),
                    newPersonLocationIp,
                    newPersonBrowser,
                    newPersonCityId,
                    newPersonLanguages,
                    newPersonEmails,
                    newPersonTags,
                    newPersonUniversities,
                    newPersonCompanies );

            neo4jUpdate1Impl( connection, addPerson );

            // Add friendship

            long person1Id = 0;
            long person2Id = 4;
            long friendshipCreationDate = 1;
            LdbcUpdate8AddFriendship addFriendship = new LdbcUpdate8AddFriendship(
                    person1Id,
                    person2Id,
                    new Date( friendshipCreationDate )
            );

            neo4jUpdate8Impl( connection, addFriendship );

            /*
            ----- Rerun same query and test for changed result -----
             */

            personId = 0;
            friendName = "name0";
            limit = 6;
            operation = new LdbcQuery1( personId, friendName, limit );

            results = neo4jLongQuery1Impl( connection, operation ).iterator();

            actualRow = results.next();
            expectedRow = new LdbcQuery1Result(
                    2L,
                    "last0-\u16a0\u3055\u4e35\u05e4\u0634",
                    1,
                    2L,
                    2L,
                    "gender2",
                    "browser2",
                    "ip2",
                    Sets.<String>newHashSet(),
                    Sets.newHashSet( "friend2language0", "friend2language1" ),
                    "city1",
                    Sets.<List<Object>>newHashSet( Lists.<Object>newArrayList( "uni2", 3, "city0" ) ),
                    Sets.<List<Object>>newHashSet()
            );
            assertThat( actualRow, equalTo( expectedRow ) );

            actualRow = results.next();
            expectedRow = new LdbcQuery1Result(
                    3L,
                    "last0-\u16a0\u3055\u4e35\u05e4\u0634",
                    1,
                    3L,
                    3L,
                    "gender3",
                    "browser3",
                    "ip3",
                    Sets.newHashSet( "friend3email1", "friend3email2" ),
                    Sets.newHashSet( "friend3language0" ),
                    "city1",
                    Lists.<List<Object>>newArrayList(),
                    Lists.<List<Object>>newArrayList( Lists.<Object>newArrayList( "company0", 1, "country0" ) )
            );
            assertThat( actualRow, equalTo( expectedRow ) );

            actualRow = results.next();
            expectedRow = new LdbcQuery1Result(
                    1L,
                    "last1-\u16a0\u3055\u4e35\u05e4\u0634",
                    1,
                    1L,
                    1L,
                    "gender1",
                    "browser1",
                    "ip1",
                    Sets.newHashSet( "friend1email1", "friend1email2" ),
                    Sets.newHashSet( "friend1language0" ),
                    "city0",
                    Lists.<List<Object>>newArrayList( Lists.<Object>newArrayList( "uni0", 0, "city1" ) ),
                    Lists.<List<Object>>newArrayList( Lists.<Object>newArrayList( "company0", 0, "country0" ) )
            );
            assertThat( actualRow, equalTo( expectedRow ) );

            actualRow = results.next();
            expectedRow = new LdbcQuery1Result(
                    4L,
                    "last4-\u16a0\u3055\u4e35\u05e4\u0634",
                    1,
                    4L,
                    4L,
                    "gender4",
                    "browser4",
                    "ip4",
                    Sets.newHashSet( "friend4email1", "friend4email2" ),
                    Sets.newHashSet( "friend4language0" ),
                    "city0",
                    Lists.<List<Object>>newArrayList( Lists.<Object>newArrayList( "uni1", 4, "city0" ) ),
                    Lists.<List<Object>>newArrayList( Lists.<Object>newArrayList( "company0", 4, "country0" ) )
            );
            assertThat( actualRow, equalTo( expectedRow ) );

            actualRow = results.next();
            expectedRow = new LdbcQuery1Result(
                    11L,
                    "last11-\u16a0\u3055\u4e35\u05e4\u0634",
                    2,
                    11L,
                    11L,
                    "gender11",
                    "browser11",
                    "ip11",
                    Sets.<String>newHashSet(),
                    Sets.<String>newHashSet(),
                    "city0",
                    Lists.<List<Object>>newArrayList( Lists.<Object>newArrayList( "uni1", 1, "city0" ),
                            Lists.<Object>newArrayList( "uni2", 2, "city0" ) ),
                    Lists.<List<Object>>newArrayList()
            );
            assertThat( actualRow, equalTo( expectedRow ) );

            actualRow = results.next();
            expectedRow = new LdbcQuery1Result(
                    31L,
                    "last31-\u16a0\u3055\u4e35\u05e4\u0634",
                    2,
                    31L,
                    31L,
                    "gender31",
                    "browser31",
                    "ip31",
                    Sets.<String>newHashSet(),
                    Sets.<String>newHashSet(),
                    "city1",
                    Lists.<List<Object>>newArrayList(),
                    Lists.<List<Object>>newArrayList()
            );
            assertThat( actualRow, equalTo( expectedRow ) );

            assertThat( results.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void query2ShouldReturnExpectedResult() throws Exception
    {

        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.Query2GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            LdbcQuery2 operation;
            Iterator<LdbcQuery2Result> results;
            LdbcQuery2Result actualRow;
            LdbcQuery2Result expectedRow;

            long personId;
            Date maxDate;
            int limit;

            personId = 1;
            maxDate = new Date( 3 );
            limit = 10;
            operation = new LdbcQuery2( personId, maxDate, limit );
            results = neo4jLongQuery2Impl( connection, operation ).iterator();

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3-\u16a0\u3055\u4e35\u05e4\u0634",
                    2,
                    "[f3Post2] content",
                    3 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3-\u16a0\u3055\u4e35\u05e4\u0634",
                    3,
                    "[f3Post3] image",
                    3 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3-\u16a0\u3055\u4e35\u05e4\u0634",
                    16,
                    "[f3Comment1] content",
                    3 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    2,
                    "f2",
                    "last2-\u16a0\u3055\u4e35\u05e4\u0634",
                    6,
                    "[f2Post2] content",
                    2 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    2,
                    "f2",
                    "last2-\u16a0\u3055\u4e35\u05e4\u0634",
                    7,
                    "[f2Post3] content",
                    2 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    2,
                    "f2",
                    "last2-\u16a0\u3055\u4e35\u05e4\u0634",
                    13,
                    "[f2Comment1] content",
                    2 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            assertThat( results.hasNext(), is( false ) );

            personId = 1;
            maxDate = new Date( 3 );
            limit = 1;
            operation = new LdbcQuery2( personId, maxDate, limit );
            results = neo4jLongQuery2Impl( connection, operation ).iterator();

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3-\u16a0\u3055\u4e35\u05e4\u0634",
                    2,
                    "[f3Post2] content",
                    3 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            assertThat( results.hasNext(), is( false ) );

            personId = 1;
            maxDate = new Date( 4 );
            limit = 5;
            operation = new LdbcQuery2( personId, maxDate, limit );
            results = neo4jLongQuery2Impl( connection, operation ).iterator();
            assertThat( results.hasNext(), is( true ) );

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3-\u16a0\u3055\u4e35\u05e4\u0634",
                    1,
                    "[f3Post1] content",
                    4 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    4,
                    "f4",
                    "last4-\u16a0\u3055\u4e35\u05e4\u0634",
                    4,
                    "[f4Post1] content",
                    4 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    2,
                    "f2",
                    "last2-\u16a0\u3055\u4e35\u05e4\u0634",
                    5,
                    "[f2Post1] content",
                    4 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    2,
                    "f2",
                    "last2-\u16a0\u3055\u4e35\u05e4\u0634",
                    14,
                    "[f2Comment2] content",
                    4 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3-\u16a0\u3055\u4e35\u05e4\u0634",
                    2,
                    "[f3Post2] content",
                    3 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            assertThat( results.hasNext(), is( false ) );

            /*
            ----- Apply updates to add person and friendship -----
             */

            long postId = 19;
            String imageFile = "imageFile19";
            Date creationDate = new Date( 2 );
            String locationIp = "ip19";
            String browserUsed = "browser19";
            String language = "language19";
            String content = "";
            int length = 0;
            long authorPersonId = 3;
            long forumId = 1;
            long countryId = 10;
            List<Long> tagIds = new ArrayList<>();
            LdbcUpdate6AddPost addPost = new LdbcUpdate6AddPost(
                    postId,
                    imageFile,
                    creationDate,
                    locationIp,
                    browserUsed,
                    language,
                    content,
                    length,
                    authorPersonId,
                    forumId,
                    countryId,
                    tagIds
            );

            neo4jUpdate6Impl( connection, addPost );

            long commentId = 18;
            creationDate = new Date( 3 );
            locationIp = "ip18";
            browserUsed = "browser18";
            content = "[f2Comment3] content";
            length = content.length();
            authorPersonId = 2;
            countryId = 10;
            long replyToPostId = 19;
            long replyToCommentId = -1;
            tagIds = new ArrayList<>();
            LdbcUpdate7AddComment addComment = new LdbcUpdate7AddComment(
                    commentId,
                    creationDate,
                    locationIp,
                    browserUsed,
                    content,
                    length,
                    authorPersonId,
                    countryId,
                    replyToPostId,
                    replyToCommentId,
                    tagIds
            );

            neo4jUpdate7Impl( connection, addComment );

            /*
            ----- Rerun same query and test for changed result -----
             */

            personId = 1;
            maxDate = new Date( 3 );
            limit = 10;
            operation = new LdbcQuery2( personId, maxDate, limit );
            results = neo4jLongQuery2Impl( connection, operation ).iterator();

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3-\u16a0\u3055\u4e35\u05e4\u0634",
                    2,
                    "[f3Post2] content",
                    3 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3-\u16a0\u3055\u4e35\u05e4\u0634",
                    3,
                    "[f3Post3] image",
                    3 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3-\u16a0\u3055\u4e35\u05e4\u0634",
                    16,
                    "[f3Comment1] content",
                    3 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    2,
                    "f2",
                    "last2-\u16a0\u3055\u4e35\u05e4\u0634",
                    18,
                    "[f2Comment3] content",
                    3 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    2,
                    "f2",
                    "last2-\u16a0\u3055\u4e35\u05e4\u0634",
                    6,
                    "[f2Post2] content",
                    2 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    2,
                    "f2",
                    "last2-\u16a0\u3055\u4e35\u05e4\u0634",
                    7,
                    "[f2Post3] content",
                    2 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    2,
                    "f2",
                    "last2-\u16a0\u3055\u4e35\u05e4\u0634",
                    13,
                    "[f2Comment1] content",
                    2 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3-\u16a0\u3055\u4e35\u05e4\u0634",
                    19,
                    "imageFile19",
                    2 );
            actualRow = results.next();
            assertThat( actualRow, equalTo( expectedRow ) );

            assertThat( results.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void query3ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.Query3GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            LdbcQuery3 operation;
            Iterator<LdbcQuery3Result> results;
            LdbcQuery3Result actualResult;
            long expectedPersonId;
            String expectedPersonFirstName;
            String expectedPersonLastName;
            int expectedXCount;
            int expectedYCount;
            int expectedCount;

            long personId;
            String countryXName;
            String countryYName;
            Date startDate;
            int durationDays;
            int limit;

            personId = 1;
            countryXName = "country1";
            countryYName = "country2";
            startDate = new Date( date( 2000, 1, 3 ) );
            durationDays = 2;
            limit = 10;
            operation = new LdbcQuery3( personId, countryXName, countryYName, startDate, durationDays, limit );

            results = neo4jLongQuery3Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPersonId = 2L;
            expectedPersonFirstName = "f2";
            expectedPersonLastName = "last2-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedXCount = 1;
            expectedYCount = 1;
            expectedCount = 2;
            assertThat(
                    actualResult,
                    equalTo( new LdbcQuery3Result(
                            expectedPersonId,
                            expectedPersonFirstName,
                            expectedPersonLastName,
                            expectedXCount,
                            expectedYCount,
                            expectedCount ) )
            );

            actualResult = results.next();
            expectedPersonId = 6L;
            expectedPersonFirstName = "ff6";
            expectedPersonLastName = "last6-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedXCount = 1;
            expectedYCount = 1;
            expectedCount = 2;
            assertThat(
                    actualResult,
                    equalTo( new LdbcQuery3Result(
                            expectedPersonId,
                            expectedPersonFirstName,
                            expectedPersonLastName,
                            expectedXCount,
                            expectedYCount,
                            expectedCount ) )
            );

            assertThat( results.hasNext(), is( false ) );

            personId = 1;
            countryXName = "country1";
            countryYName = "country2";
            startDate = new Date( date( 2000, 1, 3 ) );
            durationDays = 2;
            limit = 1;
            operation = new LdbcQuery3( personId, countryXName, countryYName, startDate, durationDays, limit );

            results = neo4jLongQuery3Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPersonId = 2L;
            expectedPersonFirstName = "f2";
            expectedPersonLastName = "last2-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedXCount = 1;
            expectedYCount = 1;
            expectedCount = 2;
            assertThat(
                    actualResult,
                    equalTo( new LdbcQuery3Result(
                            expectedPersonId,
                            expectedPersonFirstName,
                            expectedPersonLastName,
                            expectedXCount,
                            expectedYCount,
                            expectedCount ) )
            );

            assertThat( results.hasNext(), is( false ) );

            /*
            ----- Apply updates to add person and friendship -----
             */

            personId = 8;
            String personFirstName = "f8";
            String personLastName = "last8-\u16a0\u3055\u4e35\u05e4\u0634";
            String gender = "gender8";
            Date birthday = new Date( 8 );
            Date creationDate = new Date( 8 );
            String locationIp = "ip8";
            String browserUsed = "browser8";
            long cityId = 4;
            List<String> languages = Lists.newArrayList( "language8a", "language8b" );
            List<String> emails = Lists.newArrayList( "f8@email.com" );
            List<Long> tagIds = new ArrayList<>();
            List<LdbcUpdate1AddPerson.Organization> studyAts = new ArrayList<>();
            List<LdbcUpdate1AddPerson.Organization> workAts = new ArrayList<>();
            LdbcUpdate1AddPerson addPerson8 = new LdbcUpdate1AddPerson(
                    personId,
                    personFirstName,
                    personLastName,
                    gender,
                    birthday,
                    creationDate,
                    locationIp,
                    browserUsed,
                    cityId,
                    languages,
                    emails,
                    tagIds,
                    workAts,
                    studyAts
            );

            neo4jUpdate1Impl( connection, addPerson8 );

            long person1Id = 1;
            long person2Id = 8;
            creationDate = new Date( 1 );
            LdbcUpdate8AddFriendship addFriendshipFrom1To8 = new LdbcUpdate8AddFriendship(
                    person1Id,
                    person2Id,
                    creationDate
            );

            neo4jUpdate8Impl( connection, addFriendshipFrom1To8 );

            personId = 9;
            personFirstName = "ff9";
            personLastName = "last9-\u16a0\u3055\u4e35\u05e4\u0634";
            gender = "gender9";
            birthday = new Date( 9 );
            creationDate = new Date( 9 );
            locationIp = "ip9";
            browserUsed = "browser9";
            cityId = 5;
            languages = Lists.newArrayList( "language9a", "language9b" );
            emails = Lists.newArrayList( "ff9@email.com" );
            tagIds = new ArrayList<>();
            studyAts = new ArrayList<>();
            workAts = new ArrayList<>();
            LdbcUpdate1AddPerson addPerson9 = new LdbcUpdate1AddPerson(
                    personId,
                    personFirstName,
                    personLastName,
                    gender,
                    birthday,
                    creationDate,
                    locationIp,
                    browserUsed,
                    cityId,
                    languages,
                    emails,
                    tagIds,
                    workAts,
                    studyAts
            );

            neo4jUpdate1Impl( connection, addPerson9 );

            person1Id = 8;
            person2Id = 9;
            creationDate = new Date( 1 );
            LdbcUpdate8AddFriendship addFriendshipFrom8To9 = new LdbcUpdate8AddFriendship(
                    person1Id,
                    person2Id,
                    creationDate
            );

            neo4jUpdate8Impl( connection, addFriendshipFrom8To9 );

            long forumId = 1;
            String forumTitle = "forum1";
            creationDate = new Date( 1 );
            long moderatorPersonId = 8;
            tagIds = new ArrayList<>();
            LdbcUpdate4AddForum addForum = new LdbcUpdate4AddForum(
                    forumId,
                    forumTitle,
                    creationDate,
                    moderatorPersonId,
                    tagIds
            );

            neo4jUpdate4Impl( connection, addForum );

            personId = 6;
            forumId = 1;
            Date joinDate = new Date( 1 );
            LdbcUpdate5AddForumMembership addForumMembership = new LdbcUpdate5AddForumMembership(
                    forumId,
                    personId,
                    joinDate
            );

            neo4jUpdate5Impl( connection, addForumMembership );

            long postId = 19;
            String imageFile = "image19";
            creationDate = new Date( date( 2000, 1, 3 ) );
            locationIp = "ip19";
            browserUsed = "browser19";
            String language = "language19";
            String content = "content19";
            int length = content.length();
            long authorPersonId = 6;
            forumId = 1;
            long countryId = 11;
            tagIds = new ArrayList<>();
            LdbcUpdate6AddPost addPostF8 = new LdbcUpdate6AddPost(
                    postId,
                    imageFile,
                    creationDate,
                    locationIp,
                    browserUsed,
                    language,
                    content,
                    length,
                    authorPersonId,
                    forumId,
                    countryId,
                    tagIds
            );

            neo4jUpdate6Impl( connection, addPostF8 );

            personId = 9;
            forumId = 1;
            joinDate = new Date( 1 );
            addForumMembership = new LdbcUpdate5AddForumMembership(
                    forumId,
                    personId,
                    joinDate
            );

            neo4jUpdate5Impl( connection, addForumMembership );

            postId = 20;
            imageFile = "image20";
            creationDate = new Date( date( 2000, 1, 3 ) );
            locationIp = "ip20";
            browserUsed = "browser20";
            language = "language20";
            content = "content20";
            length = content.length();
            authorPersonId = 9;
            forumId = 1;
            countryId = 12;
            tagIds = new ArrayList<>();
            LdbcUpdate6AddPost addPostFf9 = new LdbcUpdate6AddPost(
                    postId,
                    imageFile,
                    creationDate,
                    locationIp,
                    browserUsed,
                    language,
                    content,
                    length,
                    authorPersonId,
                    forumId,
                    countryId,
                    tagIds
            );

            neo4jUpdate6Impl( connection, addPostFf9 );

            long commentId = 21;
            creationDate = new Date( date( 2000, 1, 3 ) );
            locationIp = "ip21";
            browserUsed = "browser21";
            content = "content21";
            length = content.length();
            authorPersonId = 9;
            countryId = 11;
            long replyToPostId = 19;
            long replyToCommentId = -1;
            tagIds = new ArrayList<>();
            LdbcUpdate7AddComment addCommentFf9 = new LdbcUpdate7AddComment(
                    commentId,
                    creationDate,
                    locationIp,
                    browserUsed,
                    content,
                    length,
                    authorPersonId,
                    countryId,
                    replyToPostId,
                    replyToCommentId,
                    tagIds
            );

            neo4jUpdate7Impl( connection, addCommentFf9 );

            /*
            ----- Rerun same query and test for changed result -----
             */

            personId = 1;
            countryXName = "country1";
            countryYName = "country2";
            startDate = new Date( date( 2000, 1, 3 ) );
            durationDays = 2;
            limit = 10;
            operation = new LdbcQuery3( personId, countryXName, countryYName, startDate, durationDays, limit );

            results = neo4jLongQuery3Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPersonId = 6L;
            expectedPersonFirstName = "ff6";
            expectedPersonLastName = "last6-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedXCount = 2;
            expectedYCount = 1;
            expectedCount = 3;
            assertThat(
                    actualResult,
                    equalTo( new LdbcQuery3Result(
                            expectedPersonId,
                            expectedPersonFirstName,
                            expectedPersonLastName,
                            expectedXCount,
                            expectedYCount,
                            expectedCount ) )
            );

            actualResult = results.next();
            expectedPersonId = 2L;
            expectedPersonFirstName = "f2";
            expectedPersonLastName = "last2-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedXCount = 1;
            expectedYCount = 1;
            expectedCount = 2;
            assertThat(
                    actualResult,
                    equalTo( new LdbcQuery3Result(
                            expectedPersonId,
                            expectedPersonFirstName,
                            expectedPersonLastName,
                            expectedXCount,
                            expectedYCount,
                            expectedCount ) )
            );

            actualResult = results.next();
            expectedPersonId = 9L;
            expectedPersonFirstName = "ff9";
            expectedPersonLastName = "last9-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedXCount = 1;
            expectedYCount = 1;
            expectedCount = 2;
            assertThat(
                    actualResult,
                    equalTo( new LdbcQuery3Result(
                            expectedPersonId,
                            expectedPersonFirstName,
                            expectedPersonLastName,
                            expectedXCount,
                            expectedYCount,
                            expectedCount ) )
            );

            assertThat( results.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void query4ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.Query4GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId;
            Date startDate;
            int durationDays;
            int limit;
            LdbcQuery4 operation;

            Iterator<LdbcQuery4Result> results;
            LdbcQuery4Result actualResult;
            String expectedTagName;
            int expectedTagCount;

            personId = 1;
            startDate = new Date( date( 2000, 1, 3 ) );
            durationDays = 2;
            limit = 10;
            operation = new LdbcQuery4( personId, startDate, durationDays, limit );

            results = neo4jLongQuery4Impl( connection, operation ).iterator();

            expectedTagName = "tag2-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedTagCount = 3;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery4Result( expectedTagName, expectedTagCount ) ) );

            expectedTagName = "tag3-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedTagCount = 2;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery4Result( expectedTagName, expectedTagCount ) ) );

            expectedTagName = "tag5-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedTagCount = 1;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery4Result( expectedTagName, expectedTagCount ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId = 1;
            startDate = new Date( date( 2000, 1, 3 ) );
            durationDays = 2;
            limit = 1;
            operation = new LdbcQuery4( personId, startDate, durationDays, limit );

            results = neo4jLongQuery4Impl( connection, operation ).iterator();

            expectedTagName = "tag2-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedTagCount = 3;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery4Result( expectedTagName, expectedTagCount ) ) );

            assertThat( results.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void query5ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.Query5GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId;
            Date joinDate;
            int limit;
            LdbcQuery5 operation;

            Iterator<LdbcQuery5Result> results;
            LdbcQuery5Result actualResult;
            String expectedForumTitle;
            int expectedPostCount;

            personId = 1;
            joinDate = new Date( date( 2000, 1, 2 ) );
            limit = 4;
            operation = new LdbcQuery5( personId, joinDate, limit );

            results = neo4jLongQuery5Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedForumTitle = "forum1-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 1;
            assertThat( actualResult, equalTo( new LdbcQuery5Result( expectedForumTitle, expectedPostCount ) ) );

            actualResult = results.next();
            expectedForumTitle = "forum3-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 1;
            assertThat( actualResult, equalTo( new LdbcQuery5Result( expectedForumTitle, expectedPostCount ) ) );

            // Note, it is totally valid for different forums to have the same title. Query should handle that case
            actualResult = results.next();
            expectedForumTitle = "forum1-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 0;
            assertThat( actualResult, equalTo( new LdbcQuery5Result( expectedForumTitle, expectedPostCount ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId = 1;
            joinDate = new Date( date( 2000, 1, 2 ) );
            limit = 1;
            operation = new LdbcQuery5( personId, joinDate, limit );

            results = neo4jLongQuery5Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedForumTitle = "forum1-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 1;
            assertThat( actualResult, equalTo( new LdbcQuery5Result( expectedForumTitle, expectedPostCount ) ) );

            assertThat( results.hasNext(), is( false ) );

            /*
            ----- Apply updates to add forum and forum memberships and posts -----
             */

            // Add forum

            long forumId = 5;
            String forumTitle = "forum5-\u16a0\u3055\u4e35\u05e4\u0634";
            Date creationDate = new Date( date( 1999, 12, 15 ) );
            long moderatorPersonId = 4;
            List<Long> tagIds = new ArrayList<>();
            LdbcUpdate4AddForum addForum = new LdbcUpdate4AddForum(
                    forumId,
                    forumTitle,
                    creationDate,
                    moderatorPersonId,
                    tagIds
            );

            neo4jUpdate4Impl( connection, addForum );

            // Add forum memberships

            forumId = 5;
            personId = 2;
            creationDate = new Date( date( 2000, 1, 4 ) );
            LdbcUpdate5AddForumMembership addForumMembership1 = new LdbcUpdate5AddForumMembership(
                    forumId,
                    personId,
                    creationDate
            );

            neo4jUpdate5Impl( connection, addForumMembership1 );

            forumId = 5;
            personId = 3;
            creationDate = new Date( date( 2000, 1, 5 ) );
            LdbcUpdate5AddForumMembership addForumMembership2 = new LdbcUpdate5AddForumMembership(
                    forumId,
                    personId,
                    creationDate
            );

            neo4jUpdate5Impl( connection, addForumMembership2 );

            // Add posts

            long postId = 13;
            String imageFile = "image13";
            creationDate = new Date( date( 2000, 1, 5 ) );
            String locationIp = "ip13";
            String browserUsed = "browser13";
            String language = "language13";
            String content = "[f2Post4] content";
            int length = content.length();
            long authorPersonId = 2;
            long countryId = 10;
            tagIds = new ArrayList<>();
            LdbcUpdate6AddPost addPost1 = new LdbcUpdate6AddPost(
                    postId,
                    imageFile,
                    creationDate,
                    locationIp,
                    browserUsed,
                    language,
                    content,
                    length,
                    authorPersonId,
                    forumId,
                    countryId,
                    tagIds
            );

            neo4jUpdate6Impl( connection, addPost1 );

            postId = 14;
            imageFile = "image14";
            creationDate = new Date( date( 2000, 1, 5 ) );
            locationIp = "ip14";
            browserUsed = "browser14";
            language = "language14";
            content = "[f3Post4] content";
            length = content.length();
            authorPersonId = 3;
            countryId = 11;
            tagIds = new ArrayList<>();
            LdbcUpdate6AddPost addPost2 = new LdbcUpdate6AddPost(
                    postId,
                    imageFile,
                    creationDate,
                    locationIp,
                    browserUsed,
                    language,
                    content,
                    length,
                    authorPersonId,
                    forumId,
                    countryId,
                    tagIds
            );

            neo4jUpdate6Impl( connection, addPost2 );

            /*
            ----- Rerun query on updated graph -----
             */

            personId = 1;
            joinDate = new Date( date( 2000, 1, 2 ) );
            limit = 4;
            operation = new LdbcQuery5( personId, joinDate, limit );

            results = neo4jLongQuery5Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedForumTitle = "forum5-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 2;
            assertThat( actualResult, equalTo( new LdbcQuery5Result( expectedForumTitle, expectedPostCount ) ) );

            actualResult = results.next();
            expectedForumTitle = "forum1-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 1;
            assertThat( actualResult, equalTo( new LdbcQuery5Result( expectedForumTitle, expectedPostCount ) ) );

            actualResult = results.next();
            expectedForumTitle = "forum3-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 1;
            assertThat( actualResult, equalTo( new LdbcQuery5Result( expectedForumTitle, expectedPostCount ) ) );

            // Note, it is totally valid for different forums to have the same title. Query should handle that case
            actualResult = results.next();
            expectedForumTitle = "forum1-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 0;
            assertThat( actualResult, equalTo( new LdbcQuery5Result( expectedForumTitle, expectedPostCount ) ) );

            assertThat( results.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void query6ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.Query6GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId;
            String tagName;
            int limit;
            LdbcQuery6 operation;
            Iterator<LdbcQuery6Result> results;
            String expectedTagName;
            int expectedPostCount;
            LdbcQuery6Result actualResult;

            personId = 1;
            tagName = "tag3-\u16a0\u3055\u4e35\u05e4\u0634";
            limit = 4;
            operation = new LdbcQuery6( personId, tagName, limit );
            results = neo4jLongQuery6Impl( connection, operation ).iterator();

            expectedTagName = "tag2-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 2;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery6Result( expectedTagName, expectedPostCount ) ) );

            expectedTagName = "tag5-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 2;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery6Result( expectedTagName, expectedPostCount ) ) );

            expectedTagName = "tag1-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 1;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery6Result( expectedTagName, expectedPostCount ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId = 1;
            tagName = "tag3-\u16a0\u3055\u4e35\u05e4\u0634";
            limit = 1;
            operation = new LdbcQuery6( personId, tagName, limit );
            results = neo4jLongQuery6Impl( connection, operation ).iterator();

            expectedTagName = "tag2-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 2;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery6Result( expectedTagName, expectedPostCount ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId = 1;
            tagName = "tag1-\u16a0\u3055\u4e35\u05e4\u0634";
            limit = 10;
            operation = new LdbcQuery6( personId, tagName, limit );
            results = neo4jLongQuery6Impl( connection, operation ).iterator();

            expectedTagName = "tag2-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 2;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery6Result( expectedTagName, expectedPostCount ) ) );

            expectedTagName = "tag4-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 2;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery6Result( expectedTagName, expectedPostCount ) ) );

            expectedTagName = "tag3-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 1;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery6Result( expectedTagName, expectedPostCount ) ) );

            expectedTagName = "tag5-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedPostCount = 1;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery6Result( expectedTagName, expectedPostCount ) ) );

            assertThat( results.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void query7ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.Query7GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId;
            int limit;
            LdbcQuery7 operation;

            Iterator<LdbcQuery7Result> results;
            LdbcQuery7Result actualResult;
            long expectedPersonId;
            String expectedPersonFirstName;
            String expectedPersonLastName;
            long expectedLikeCreationDate;
            long expectedCommentOrPostId;
            String expectedCommentOrPostContent;
            int expectedMinutesLatency;
            boolean expectedIsNew;

            personId = 1;
            limit = 7;
            operation = new LdbcQuery7( personId, limit );
            results = neo4jLongQuery7Impl( connection, operation ).iterator();

            expectedPersonId = 8L;
            expectedPersonFirstName = "s8";
            expectedPersonLastName = "last8-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedLikeCreationDate = date( 2000, 1, 1, 0, 10, 0 );
            expectedCommentOrPostId = 2L;
            expectedCommentOrPostContent = "person1post2";
            expectedMinutesLatency = 8;
            expectedIsNew = true;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery7Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedLikeCreationDate,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedMinutesLatency,
                    expectedIsNew ) )
            );

            expectedPersonId = 7L;
            expectedPersonFirstName = "s7";
            expectedPersonLastName = "last7-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedLikeCreationDate = date( 2000, 1, 1, 0, 6, 0 );
            expectedCommentOrPostId = 5L;
            expectedCommentOrPostContent = "person1comment1";
            expectedMinutesLatency = 1;
            expectedIsNew = true;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery7Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedLikeCreationDate,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedMinutesLatency,
                    expectedIsNew ) )
            );

            expectedPersonId = 2L;
            expectedPersonFirstName = "f2";
            expectedPersonLastName = "last2-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedLikeCreationDate = date( 2000, 1, 1, 0, 5, 0 );
            expectedCommentOrPostId = 1L;
            expectedCommentOrPostContent = "person1post1";
            expectedMinutesLatency = 4;
            expectedIsNew = false;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery7Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedLikeCreationDate,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedMinutesLatency,
                    expectedIsNew ) )
            );

            expectedPersonId = 4L;
            expectedPersonFirstName = "f4";
            expectedPersonLastName = "last4-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedLikeCreationDate = date( 2000, 1, 1, 0, 5, 0 );
            expectedCommentOrPostId = 3L;
            expectedCommentOrPostContent = "person1post3";
            expectedMinutesLatency = 2;
            expectedIsNew = false;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery7Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedLikeCreationDate,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedMinutesLatency,
                    expectedIsNew ) )
            );

            expectedPersonId = 6L;
            expectedPersonFirstName = "ff6";
            expectedPersonLastName = "last6-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedLikeCreationDate = date( 2000, 1, 1, 0, 4, 0 );
            expectedCommentOrPostId = 1L;
            expectedCommentOrPostContent = "person1post1";
            expectedMinutesLatency = 3;
            expectedIsNew = true;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery7Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedLikeCreationDate,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedMinutesLatency,
                    expectedIsNew ) )
            );

            expectedPersonId = 1L;
            expectedPersonFirstName = "person1";
            expectedPersonLastName = "last1-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedLikeCreationDate = date( 2000, 1, 1, 0, 1, 0 );
            expectedCommentOrPostId = 1L;
            expectedCommentOrPostContent = "person1post1";
            expectedMinutesLatency = 0;
            expectedIsNew = true;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery7Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedLikeCreationDate,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedMinutesLatency,
                    expectedIsNew ) )
            );

            assertThat( results.hasNext(), is( false ) );

            personId = 1;
            limit = 1;
            operation = new LdbcQuery7( personId, limit );
            results = neo4jLongQuery7Impl( connection, operation ).iterator();

            expectedPersonId = 8L;
            expectedPersonFirstName = "s8";
            expectedPersonLastName = "last8-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedLikeCreationDate = date( 2000, 1, 1, 0, 10, 0 );
            expectedCommentOrPostId = 2L;
            expectedCommentOrPostContent = "person1post2";
            expectedMinutesLatency = 8;
            expectedIsNew = true;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery7Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedLikeCreationDate,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedMinutesLatency,
                    expectedIsNew ) )
            );

            assertThat( results.hasNext(), is( false ) );

            /*
            ----- Apply updates to add person and friendship -----
             */

            // Add person

            long newPersonId = 9;
            String newPersonFirstName = "s9";
            String newPersonLastName = "last9-\u16a0\u3055\u4e35\u05e4\u0634";
            String newPersonGender = "gender9";
            long newPersonBirthday = 9L;
            long newPersonCreationDate = 9L;
            String newPersonLocationIp = "ip9";
            String newPersonBrowser = "browser9";
            long newPersonCityId = 0;
            List<String> newPersonLanguages = Lists.newArrayList( "friend9language0" );
            List<String> newPersonEmails = Lists.newArrayList( "friend9email1", "friend9email2" );
            List<Long> newPersonTags = new ArrayList<>();
            List<LdbcUpdate1AddPerson.Organization> newPersonUniversities = new ArrayList<>();
            List<LdbcUpdate1AddPerson.Organization> newPersonCompanies = new ArrayList<>();
            LdbcUpdate1AddPerson addPerson = new LdbcUpdate1AddPerson(
                    newPersonId,
                    newPersonFirstName,
                    newPersonLastName,
                    newPersonGender,
                    new Date( newPersonBirthday ),
                    new Date( newPersonCreationDate ),
                    newPersonLocationIp,
                    newPersonBrowser,
                    newPersonCityId,
                    newPersonLanguages,
                    newPersonEmails,
                    newPersonTags,
                    newPersonUniversities,
                    newPersonCompanies );

            neo4jUpdate1Impl( connection, addPerson );

            // Add post like

            long likerPersonId = 9;
            long likedPostId = 1;
            Date likeCreationDate = new Date( date( 2000, 1, 1, 0, 9, 0 ) );
            LdbcUpdate2AddPostLike addPostLike = new LdbcUpdate2AddPostLike(
                    likerPersonId,
                    likedPostId,
                    likeCreationDate );

            neo4jUpdate2Impl( connection, addPostLike );

            // Add comment like

            likerPersonId = 7;
            long likedCommentId = 5;
            likeCreationDate = new Date( date( 2000, 1, 1, 0, 8, 0 ) );
            LdbcUpdate3AddCommentLike addCommentLike = new LdbcUpdate3AddCommentLike(
                    likerPersonId,
                    likedCommentId,
                    likeCreationDate );

            neo4jUpdate3Impl( connection, addCommentLike );

            /*
            ----- Rerun query on updated graph -----
             */

            personId = 1;
            limit = 7;
            operation = new LdbcQuery7( personId, limit );
            results = neo4jLongQuery7Impl( connection, operation ).iterator();

            expectedPersonId = 8L;
            expectedPersonFirstName = "s8";
            expectedPersonLastName = "last8-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedLikeCreationDate = date( 2000, 1, 1, 0, 10, 0 );
            expectedCommentOrPostId = 2L;
            expectedCommentOrPostContent = "person1post2";
            expectedMinutesLatency = 8;
            expectedIsNew = true;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery7Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedLikeCreationDate,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedMinutesLatency,
                    expectedIsNew ) )
            );

            expectedPersonId = 9L;
            expectedPersonFirstName = "s9";
            expectedPersonLastName = "last9-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedLikeCreationDate = date( 2000, 1, 1, 0, 9, 0 );
            expectedCommentOrPostId = 1L;
            expectedCommentOrPostContent = "person1post1";
            expectedMinutesLatency = 8;
            expectedIsNew = true;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery7Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedLikeCreationDate,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedMinutesLatency,
                    expectedIsNew ) )
            );

            expectedPersonId = 7L;
            expectedPersonFirstName = "s7";
            expectedPersonLastName = "last7-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedLikeCreationDate = date( 2000, 1, 1, 0, 8, 0 );
            expectedCommentOrPostId = 5L;
            expectedCommentOrPostContent = "person1comment1";
            expectedMinutesLatency = 3;
            expectedIsNew = true;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery7Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedLikeCreationDate,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedMinutesLatency,
                    expectedIsNew ) )
            );

            expectedPersonId = 2L;
            expectedPersonFirstName = "f2";
            expectedPersonLastName = "last2-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedLikeCreationDate = date( 2000, 1, 1, 0, 5, 0 );
            expectedCommentOrPostId = 1L;
            expectedCommentOrPostContent = "person1post1";
            expectedMinutesLatency = 4;
            expectedIsNew = false;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery7Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedLikeCreationDate,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedMinutesLatency,
                    expectedIsNew ) )
            );

            expectedPersonId = 4L;
            expectedPersonFirstName = "f4";
            expectedPersonLastName = "last4-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedLikeCreationDate = date( 2000, 1, 1, 0, 5, 0 );
            expectedCommentOrPostId = 3L;
            expectedCommentOrPostContent = "person1post3";
            expectedMinutesLatency = 2;
            expectedIsNew = false;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery7Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedLikeCreationDate,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedMinutesLatency,
                    expectedIsNew ) )
            );

            expectedPersonId = 6L;
            expectedPersonFirstName = "ff6";
            expectedPersonLastName = "last6-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedLikeCreationDate = date( 2000, 1, 1, 0, 4, 0 );
            expectedCommentOrPostId = 1L;
            expectedCommentOrPostContent = "person1post1";
            expectedMinutesLatency = 3;
            expectedIsNew = true;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery7Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedLikeCreationDate,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedMinutesLatency,
                    expectedIsNew ) )
            );

            expectedPersonId = 1L;
            expectedPersonFirstName = "person1";
            expectedPersonLastName = "last1-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedLikeCreationDate = date( 2000, 1, 1, 0, 1, 0 );
            expectedCommentOrPostId = 1L;
            expectedCommentOrPostContent = "person1post1";
            expectedMinutesLatency = 0;
            expectedIsNew = true;
            actualResult = results.next();
            assertThat( actualResult, equalTo( new LdbcQuery7Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedLikeCreationDate,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedMinutesLatency,
                    expectedIsNew ) )
            );

            assertThat( results.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void query8ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.Query8GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId;
            int limit;
            LdbcQuery8 operation;

            Iterator<LdbcQuery8Result> results;
            LdbcQuery8Result actualResult;
            long expectedPersonId;
            String expectedPersonFirstName;
            String expectedPersonLastName;
            long expectedCommentId;
            long expectedCommentDate;
            String expectedCommentContent;

            personId = 0;
            limit = 10;
            operation = new LdbcQuery8( personId, limit );
            results = neo4jLongQuery8Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPersonId = 1L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentDate = 7L;
            expectedCommentId = 17L;
            expectedCommentContent = "C21";
            assertThat( actualResult, equalTo( new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent ) ) );

            actualResult = results.next();
            expectedPersonId = 2L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "two-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentDate = 4L;
            expectedCommentId = 14L;
            expectedCommentContent = "C131";
            assertThat( actualResult, equalTo( new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent ) ) );

            actualResult = results.next();
            expectedPersonId = 0L;
            expectedPersonFirstName = "person";
            expectedPersonLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentDate = 3L;
            expectedCommentId = 13L;
            expectedCommentContent = "C13";
            assertThat( actualResult, equalTo( new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent ) ) );

            actualResult = results.next();
            expectedPersonId = 3L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "three-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentDate = 2L;
            expectedCommentId = 12L;
            expectedCommentContent = "C12";
            assertThat( actualResult, equalTo( new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent ) ) );

            actualResult = results.next();
            expectedPersonId = 3L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "three-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentDate = 1L;
            expectedCommentId = 10L;
            expectedCommentContent = "C01";
            assertThat( actualResult, equalTo( new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent ) ) );

            actualResult = results.next();
            expectedPersonId = 3L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "three-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentDate = 1L;
            expectedCommentId = 11L;
            expectedCommentContent = "C11";
            assertThat( actualResult, equalTo( new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId = 0;
            limit = 1;
            operation = new LdbcQuery8( personId, limit );
            results = neo4jLongQuery8Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPersonId = 1L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentDate = 7L;
            expectedCommentId = 17L;
            expectedCommentContent = "C21";
            assertThat( actualResult, equalTo( new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId = 0;
            limit = 1;
            operation = new LdbcQuery8( personId, limit );
            results = neo4jLongQuery8Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPersonId = 1L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentDate = 7L;
            expectedCommentId = 17L;
            expectedCommentContent = "C21";
            assertThat( actualResult, equalTo( new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent ) ) );

            assertThat( results.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void query9ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.Query9GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId;
            long latestDateAsMilli;
            Date latestDate;
            int limit;
            LdbcQuery9 operation;

            Iterator<LdbcQuery9Result> results;
            LdbcQuery9Result actualResult;
            long expectedPersonId;
            String expectedPersonFirstName;
            String expectedPersonLastName;
            long expectedCommentOrPostId;
            String expectedCommentOrPostContent;
            long expectedCommentOrPostCreationDate;

            personId = 0;
            latestDateAsMilli = 12;
            latestDate = new Date( latestDateAsMilli );
            limit = 10;
            operation = new LdbcQuery9( personId, latestDate, limit );
            results = neo4jLongQuery9Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPersonId = 1L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentOrPostId = 11L;
            expectedCommentOrPostContent = "P11 - content";
            expectedCommentOrPostCreationDate = 11L;
            assertThat( actualResult, equalTo( new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate ) ) );

            actualResult = results.next();
            expectedPersonId = 4L;
            expectedPersonFirstName = "friendfriend";
            expectedPersonLastName = "four-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentOrPostId = 1211L;
            expectedCommentOrPostContent = "C1211";
            expectedCommentOrPostCreationDate = 10L;
            assertThat( actualResult, equalTo( new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate ) ) );

            actualResult = results.next();
            expectedPersonId = 1L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentOrPostId = 21111L;
            expectedCommentOrPostContent = "C21111";
            expectedCommentOrPostCreationDate = 9L;
            assertThat( actualResult, equalTo( new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate ) ) );

            actualResult = results.next();
            expectedPersonId = 2L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "two-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentOrPostId = 2111L;
            expectedCommentOrPostContent = "C2111";
            expectedCommentOrPostCreationDate = 8L;
            assertThat( actualResult, equalTo( new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate ) ) );

            actualResult = results.next();
            expectedPersonId = 1L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentOrPostId = 211L;
            expectedCommentOrPostContent = "C211";
            expectedCommentOrPostCreationDate = 7L;
            assertThat( actualResult, equalTo( new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate ) ) );

            actualResult = results.next();
            expectedPersonId = 2L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "two-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentOrPostId = 21L;
            expectedCommentOrPostContent = "P21 - image";
            expectedCommentOrPostCreationDate = 6L;
            assertThat( actualResult, equalTo( new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate ) ) );

            actualResult = results.next();
            expectedPersonId = 1L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentOrPostId = 12L;
            expectedCommentOrPostContent = "P12 - content";
            expectedCommentOrPostCreationDate = 4L;
            assertThat( actualResult, equalTo( new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate ) ) );

            actualResult = results.next();
            expectedPersonId = 2L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "two-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentOrPostId = 311L;
            expectedCommentOrPostContent = "C311";
            expectedCommentOrPostCreationDate = 4L;
            assertThat( actualResult, equalTo( new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId = 0;
            latestDateAsMilli = 12;
            latestDate = new Date( latestDateAsMilli );
            limit = 1;
            operation = new LdbcQuery9( personId, latestDate, limit );
            results = neo4jLongQuery9Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPersonId = 1L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommentOrPostId = 11L;
            expectedCommentOrPostContent = "P11 - content";
            expectedCommentOrPostCreationDate = 11L;
            assertThat( actualResult, equalTo( new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate ) ) );

            assertThat( results.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void query10ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.Query10GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId;
            int month;
            int limit;
            LdbcQuery10 operation;

            Iterator<LdbcQuery10Result> results;
            LdbcQuery10Result actualResult;
            long expectedPersonId;
            String expectedPersonFirstName;
            String expectedPersonLastName;
            int expectedCommonInterestScore;
            String expectedPersonGender;
            String expectedPersonCityName;

            personId = 0;
            month = 1;
            limit = 7;
            operation = new LdbcQuery10( personId, month, limit );
            results = neo4jLongQuery10Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPersonId = 22L;
            expectedPersonFirstName = "friendfriend";
            expectedPersonLastName = "two two-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommonInterestScore = 0;
            expectedPersonGender = "male";
            expectedPersonCityName = "city0";
            assertThat( actualResult, equalTo( new LdbcQuery10Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommonInterestScore,
                    expectedPersonGender,
                    expectedPersonCityName
            ) ) );

            actualResult = results.next();
            expectedPersonId = 11L;
            expectedPersonFirstName = "friendfriend";
            expectedPersonLastName = "one one-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommonInterestScore = -1;
            expectedPersonGender = "female";
            expectedPersonCityName = "city1";
            assertThat( actualResult, equalTo( new LdbcQuery10Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommonInterestScore,
                    expectedPersonGender,
                    expectedPersonCityName
            ) ) );

            actualResult = results.next();
            expectedPersonId = 12L;
            expectedPersonFirstName = "friendfriend";
            expectedPersonLastName = "one two-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommonInterestScore = -1;
            expectedPersonGender = "male";
            expectedPersonCityName = "city0";
            assertThat( actualResult, equalTo( new LdbcQuery10Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommonInterestScore,
                    expectedPersonGender,
                    expectedPersonCityName
            ) ) );

            actualResult = results.next();
            expectedPersonId = 21L;
            expectedPersonFirstName = "friendfriend";
            expectedPersonLastName = "two one-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommonInterestScore = -1;
            expectedPersonGender = "male";
            expectedPersonCityName = "city0";
            assertThat( actualResult, equalTo( new LdbcQuery10Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommonInterestScore,
                    expectedPersonGender,
                    expectedPersonCityName
            ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId = 0;
            month = 1;
            limit = 1;
            operation = new LdbcQuery10( personId, month, limit );
            results = neo4jLongQuery10Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPersonId = 22L;
            expectedPersonFirstName = "friendfriend";
            expectedPersonLastName = "two two-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedCommonInterestScore = 0;
            expectedPersonGender = "male";
            expectedPersonCityName = "city0";
            assertThat( actualResult, equalTo( new LdbcQuery10Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommonInterestScore,
                    expectedPersonGender,
                    expectedPersonCityName
            ) ) );

            assertThat( results.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void query11ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.Query11GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId;
            String countryName;
            int startedBeforeWorkFromYear;
            int limit;
            LdbcQuery11 operation;

            Iterator<LdbcQuery11Result> results;
            LdbcQuery11Result actualResult;
            long expectedPersonId;
            String expectedPersonFirstName;
            String expectedPersonLastName;
            String expectedOrganizationName;
            int expectedWorkFromYear;

            personId = 0;
            countryName = "country0";
            startedBeforeWorkFromYear = 5;
            limit = 4;
            operation = new LdbcQuery11( personId, countryName, startedBeforeWorkFromYear, limit );
            results = neo4jLongQuery11Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPersonId = 1L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedOrganizationName = "company zero";
            expectedWorkFromYear = 2;
            assertThat( actualResult, equalTo( new LdbcQuery11Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedOrganizationName,
                    expectedWorkFromYear ) ) );

            actualResult = results.next();
            expectedPersonId = 11L;
            expectedPersonFirstName = "friend friend";
            expectedPersonLastName = "one one-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedOrganizationName = "company zero";
            expectedWorkFromYear = 3;
            assertThat( actualResult, equalTo( new LdbcQuery11Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedOrganizationName,
                    expectedWorkFromYear ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId = 0;
            countryName = "country0";
            startedBeforeWorkFromYear = 5;
            limit = 1;
            operation = new LdbcQuery11( personId, countryName, startedBeforeWorkFromYear, limit );
            results = neo4jLongQuery11Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPersonId = 1L;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedOrganizationName = "company zero";
            expectedWorkFromYear = 2;
            assertThat( actualResult, equalTo( new LdbcQuery11Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedOrganizationName,
                    expectedWorkFromYear ) ) );

            assertThat( results.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void query12ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.Query12GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId;
            String tagClassName;
            int limit;
            LdbcQuery12 operation;

            Iterator<LdbcQuery12Result> results;
            LdbcQuery12Result actualResult;
            long expectedPersonId;
            String expectedPersonFirstName;
            String expectedPersonLastName;
            Iterable<String> expectedTagNames;
            int expectedReplyCount;

            personId = 0;
            tagClassName = "1";
            limit = 10;
            operation = new LdbcQuery12( personId, tagClassName, limit );
            results = neo4jLongQuery12Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPersonId = 1L;
            expectedPersonFirstName = "f";
            expectedPersonLastName = "1";
            expectedTagNames = Sets.newHashSet( "tag111-\u16a0\u3055\u4e35\u05e4\u0634",
                    "tag12111-\u16a0\u3055\u4e35\u05e4\u0634" );
            expectedReplyCount = 2;
            assertThat( actualResult, equalTo( new LdbcQuery12Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedTagNames,
                    expectedReplyCount ) ) );

            actualResult = results.next();
            expectedPersonId = 2L;
            expectedPersonFirstName = "f";
            expectedPersonLastName = "2";
            expectedTagNames = Sets.newHashSet( "tag111-\u16a0\u3055\u4e35\u05e4\u0634" );
            expectedReplyCount = 1;
            assertThat( actualResult, equalTo( new LdbcQuery12Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedTagNames,
                    expectedReplyCount ) ) );

            actualResult = results.next();
            expectedPersonId = 3L;
            expectedPersonFirstName = "f";
            expectedPersonLastName = "3";
            expectedTagNames = Sets.newHashSet( "tag11-\u16a0\u3055\u4e35\u05e4\u0634",
                    "tag12111-\u16a0\u3055\u4e35\u05e4\u0634" );
            expectedReplyCount = 1;
            assertThat( actualResult, equalTo( new LdbcQuery12Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedTagNames,
                    expectedReplyCount ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId = 0;
            tagClassName = "1";
            limit = 1;
            operation = new LdbcQuery12( personId, tagClassName, limit );
            results = neo4jLongQuery12Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPersonId = 1L;
            expectedPersonFirstName = "f";
            expectedPersonLastName = "1";
            expectedTagNames = Sets.newHashSet( "tag111-\u16a0\u3055\u4e35\u05e4\u0634",
                    "tag12111-\u16a0\u3055\u4e35\u05e4\u0634" );
            expectedReplyCount = 2;
            assertThat( actualResult, equalTo( new LdbcQuery12Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedTagNames,
                    expectedReplyCount ) ) );

            assertThat( results.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void query13ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.Query13GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId1;
            long personId2;
            LdbcQuery13 operation;

            Iterator<LdbcQuery13Result> results;
            LdbcQuery13Result actualResult;
            int expectedShortestPathLength;

            personId1 = 0;
            personId2 = 5;
            operation = new LdbcQuery13( personId1, personId2 );
            results = Lists.newArrayList( neo4jLongQuery13Impl( connection, operation ) ).iterator();

            actualResult = results.next();
            expectedShortestPathLength = 5;
            assertThat( actualResult, equalTo( new LdbcQuery13Result( expectedShortestPathLength ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId1 = 7;
            personId2 = 3;
            operation = new LdbcQuery13( personId1, personId2 );
            results = Lists.newArrayList( neo4jLongQuery13Impl( connection, operation ) ).iterator();

            actualResult = results.next();
            expectedShortestPathLength = 3;
            assertThat( actualResult, equalTo( new LdbcQuery13Result( expectedShortestPathLength ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId1 = 1;
            personId2 = 2;
            operation = new LdbcQuery13( personId1, personId2 );
            results = Lists.newArrayList( neo4jLongQuery13Impl( connection, operation ) ).iterator();

            actualResult = results.next();
            expectedShortestPathLength = 1;
            assertThat( actualResult, equalTo( new LdbcQuery13Result( expectedShortestPathLength ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId1 = 1;
            personId2 = 1;
            operation = new LdbcQuery13( personId1, personId2 );
            results = Lists.newArrayList( neo4jLongQuery13Impl( connection, operation ) ).iterator();

            actualResult = results.next();
            expectedShortestPathLength = 0;
            assertThat( actualResult, equalTo( new LdbcQuery13Result( expectedShortestPathLength ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId1 = 1;
            personId2 = 8;
            operation = new LdbcQuery13( personId1, personId2 );
            results = Lists.newArrayList( neo4jLongQuery13Impl( connection, operation ) ).iterator();

            actualResult = results.next();
            expectedShortestPathLength = -1;
            assertThat( actualResult, equalTo( new LdbcQuery13Result( expectedShortestPathLength ) ) );

            assertThat( results.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void query14ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.Query14GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId1;
            long personId2;
            LdbcQuery14 operation;

            Iterator<LdbcQuery14Result> results;
            LdbcQuery14Result actualResult;
            Collection<Long> expectedPathNodeIds;
            double expectedWeight;

            personId1 = 0;
            personId2 = 5;
            operation = new LdbcQuery14( personId1, personId2 );
            results = neo4jLongQuery14Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList( 0L, 1L, 7L, 4L, 8L, 5L );
            expectedWeight = 5.5;
            assertThat( actualResult, equalTo( new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            ) ) );

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList( 0L, 1L, 7L, 4L, 6L, 5L );
            expectedWeight = 4.5;
            assertThat( actualResult, equalTo( new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            ) ) );

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList( 0L, 1L, 2L, 4L, 8L, 5L );
            expectedWeight = 4.0;
            assertThat( actualResult, equalTo( new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            ) ) );

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList( 0L, 1L, 2L, 4L, 6L, 5L );
            expectedWeight = 3.0;
            assertThat( actualResult, equalTo( new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId1 = 5;
            personId2 = 5;
            operation = new LdbcQuery14( personId1, personId2 );
            results = neo4jLongQuery14Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList( 5L );
            expectedWeight = 0;
            assertThat( actualResult, equalTo( new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            ) ) );

            assertThat( results.hasNext(), is( false ) );

            personId1 = 0;
            personId2 = 9;
            operation = new LdbcQuery14( personId1, personId2 );
            results = neo4jLongQuery14Impl( connection, operation ).iterator();

            assertThat( results.hasNext(), is( false ) );

            // Add a comment in a conversation between p5 and p6, a comment in reply to a comment

            long p6Comment1 = 16;
            Date creationDate = new Date( 1 );
            String locationIp = "locationIp";
            String browserUsed = "browserUsed";
            String content = "nothing";
            int length = 1;
            long authorPersonId = 6;
            long countryId = 0;
            long replyToPostId = -1;
            // (p5Comment2:Comment:Message {Message.ID:12})
            long replyToCommentId = 12;
            List<Long> tagIds = new ArrayList<>();
            LdbcUpdate7AddComment update = new LdbcUpdate7AddComment(
                    p6Comment1,
                    creationDate,
                    locationIp,
                    browserUsed,
                    content,
                    length,
                    authorPersonId,
                    countryId,
                    replyToPostId,
                    replyToCommentId,
                    tagIds
            );
            neo4jUpdate7Impl( connection, update );

            personId1 = 0;
            personId2 = 5;
            operation = new LdbcQuery14( personId1, personId2 );
            results = neo4jLongQuery14Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList( 0L, 1L, 7L, 4L, 8L, 5L );
            expectedWeight = 5.5;
            assertThat( actualResult, equalTo( new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            ) ) );

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList( 0L, 1L, 7L, 4L, 6L, 5L );
            expectedWeight = 5.0;
            assertThat( actualResult, equalTo( new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            ) ) );

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList( 0L, 1L, 2L, 4L, 8L, 5L );
            expectedWeight = 4.0;
            assertThat( actualResult, equalTo( new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            ) ) );

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList( 0L, 1L, 2L, 4L, 6L, 5L );
            expectedWeight = 3.5;
            assertThat( actualResult, equalTo( new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            ) ) );

            assertThat( results.hasNext(), is( false ) );

            // Add another comment in a conversation between p5 and p6, a comment in reply to a post

            long p6Comment2 = 17;
            creationDate = new Date( 1 );
            locationIp = "locationIp";
            browserUsed = "browserUsed";
            content = "nothing";
            length = 1;
            authorPersonId = 6;
            countryId = 0;
            // (p5Post1:Post:Message {Message.ID:3})
            replyToPostId = 3;
            replyToCommentId = -1;
            tagIds = new ArrayList<>();
            update = new LdbcUpdate7AddComment(
                    p6Comment2,
                    creationDate,
                    locationIp,
                    browserUsed,
                    content,
                    length,
                    authorPersonId,
                    countryId,
                    replyToPostId,
                    replyToCommentId,
                    tagIds
            );
            neo4jUpdate7Impl( connection, update );

            personId1 = 0;
            personId2 = 5;
            operation = new LdbcQuery14( personId1, personId2 );
            results = neo4jLongQuery14Impl( connection, operation ).iterator();

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList( 0L, 1L, 7L, 4L, 6L, 5L );
            expectedWeight = 6.0;
            assertThat( actualResult, equalTo( new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            ) ) );

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList( 0L, 1L, 7L, 4L, 8L, 5L );
            expectedWeight = 5.5;
            assertThat( actualResult, equalTo( new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            ) ) );

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList( 0L, 1L, 2L, 4L, 6L, 5L );
            expectedWeight = 4.5;
            assertThat( actualResult, equalTo( new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            ) ) );

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList( 0L, 1L, 2L, 4L, 8L, 5L );
            expectedWeight = 4.0;
            assertThat( actualResult, equalTo( new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            ) ) );

            assertThat( results.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void shortQuery1ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.ShortQuery1GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId;
            LdbcShortQuery1PersonProfile operation;

            LdbcShortQuery1PersonProfileResult actualRow;
            LdbcShortQuery1PersonProfileResult expectedRow;

            personId = 0;
            operation = new LdbcShortQuery1PersonProfile( personId );
            actualRow = neo4jShortQuery1Impl( connection, operation );
            expectedRow = new LdbcShortQuery1PersonProfileResult(
                    "person0",
                    "zero-\u16a0\u3055\u4e35\u05e4\u0634",
                    0,
                    "ip0",
                    "browser0",
                    0,
                    "gender0",
                    0
            );
            assertThat( actualRow, equalTo( expectedRow ) );

            personId = 1;
            operation = new LdbcShortQuery1PersonProfile( personId );
            actualRow = neo4jShortQuery1Impl( connection, operation );
            expectedRow = new LdbcShortQuery1PersonProfileResult(
                    "person1",
                    "one-\u16a0\u3055\u4e35\u05e4\u0634",
                    1,
                    "ip1",
                    "browser1",
                    1,
                    "gender1",
                    1
            );
            assertThat( actualRow, equalTo( expectedRow ) );

            personId = 2;
            operation = new LdbcShortQuery1PersonProfile( personId );
            actualRow = neo4jShortQuery1Impl( connection, operation );
            expectedRow = new LdbcShortQuery1PersonProfileResult(
                    "person2",
                    "two-\u16a0\u3055\u4e35\u05e4\u0634",
                    2,
                    "ip2",
                    "browser2",
                    1,
                    "gender2",
                    2
            );
            assertThat( actualRow, equalTo( expectedRow ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void shortQuery2ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.ShortQuery2GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId;
            int limit;
            LdbcShortQuery2PersonPosts operation;

            Iterator<LdbcShortQuery2PersonPostsResult> rows;
            LdbcShortQuery2PersonPostsResult expectedRow;
            long messageId;
            String messageContent;
            long messageCreationDate;
            long postId;
            long postAuthorId;
            String postAuthorFirstName;
            String postAuthorLastName;

            personId = 0;
            limit = 10;
            operation = new LdbcShortQuery2PersonPosts( personId, limit );
            rows = neo4jShortQuery2Impl( connection, operation ).iterator();
            messageId = 5;
            messageContent = "comment_content5";
            messageCreationDate = 5;
            postId = 0;
            postAuthorId = 0;
            postAuthorFirstName = "person0";
            postAuthorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery2PersonPostsResult(
                    messageId,
                    messageContent,
                    messageCreationDate,
                    postId,
                    postAuthorId,
                    postAuthorFirstName,
                    postAuthorLastName
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            messageId = 1;
            messageContent = "post_image1";
            messageCreationDate = 1;
            postId = 1;
            postAuthorId = 0;
            postAuthorFirstName = "person0";
            postAuthorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery2PersonPostsResult(
                    messageId,
                    messageContent,
                    messageCreationDate,
                    postId,
                    postAuthorId,
                    postAuthorFirstName,
                    postAuthorLastName
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            messageId = 2;
            messageContent = "post_image2";
            messageCreationDate = 1;
            postId = 2;
            postAuthorId = 0;
            postAuthorFirstName = "person0";
            postAuthorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery2PersonPostsResult(
                    messageId,
                    messageContent,
                    messageCreationDate,
                    postId,
                    postAuthorId,
                    postAuthorFirstName,
                    postAuthorLastName
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            messageId = 0;
            messageContent = "post_content0";
            messageCreationDate = 0;
            postId = 0;
            postAuthorId = 0;
            postAuthorFirstName = "person0";
            postAuthorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery2PersonPostsResult(
                    messageId,
                    messageContent,
                    messageCreationDate,
                    postId,
                    postAuthorId,
                    postAuthorFirstName,
                    postAuthorLastName
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            assertThat( rows.hasNext(), equalTo( false ) );

            personId = 0;
            limit = 2;
            operation = new LdbcShortQuery2PersonPosts( personId, limit );
            rows = neo4jShortQuery2Impl( connection, operation ).iterator();
            messageId = 5;
            messageContent = "comment_content5";
            messageCreationDate = 5;
            postId = 0;
            postAuthorId = 0;
            postAuthorFirstName = "person0";
            postAuthorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery2PersonPostsResult(
                    messageId,
                    messageContent,
                    messageCreationDate,
                    postId,
                    postAuthorId,
                    postAuthorFirstName,
                    postAuthorLastName
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            messageId = 1;
            messageContent = "post_image1";
            messageCreationDate = 1;
            postId = 1;
            postAuthorId = 0;
            postAuthorFirstName = "person0";
            postAuthorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery2PersonPostsResult(
                    messageId,
                    messageContent,
                    messageCreationDate,
                    postId,
                    postAuthorId,
                    postAuthorFirstName,
                    postAuthorLastName
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            assertThat( rows.hasNext(), equalTo( false ) );

            personId = 1;
            limit = 10;
            operation = new LdbcShortQuery2PersonPosts( personId, limit );
            rows = neo4jShortQuery2Impl( connection, operation ).iterator();
            messageId = 4;
            messageContent = "comment_content4";
            messageCreationDate = 4;
            postId = 1;
            postAuthorId = 0;
            postAuthorFirstName = "person0";
            postAuthorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery2PersonPostsResult(
                    messageId,
                    messageContent,
                    messageCreationDate,
                    postId,
                    postAuthorId,
                    postAuthorFirstName,
                    postAuthorLastName
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            messageId = 3;
            messageContent = "comment_content3";
            messageCreationDate = 3;
            postId = 0;
            postAuthorId = 0;
            postAuthorFirstName = "person0";
            postAuthorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery2PersonPostsResult(
                    messageId,
                    messageContent,
                    messageCreationDate,
                    postId,
                    postAuthorId,
                    postAuthorFirstName,
                    postAuthorLastName
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            assertThat( rows.hasNext(), equalTo( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void shortQuery3ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.ShortQuery3GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long personId;
            LdbcShortQuery3PersonFriends operation;

            Iterator<LdbcShortQuery3PersonFriendsResult> rows;
            LdbcShortQuery3PersonFriendsResult expectedRow;

            personId = 0;
            operation = new LdbcShortQuery3PersonFriends( personId );
            rows = neo4jShortQuery3Impl( connection, operation ).iterator();
            expectedRow = new LdbcShortQuery3PersonFriendsResult(
                    3,
                    "person3",
                    "three-\u16a0\u3055\u4e35\u05e4\u0634",
                    2
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            expectedRow = new LdbcShortQuery3PersonFriendsResult(
                    1,
                    "person1",
                    "one-\u16a0\u3055\u4e35\u05e4\u0634",
                    0
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            expectedRow = new LdbcShortQuery3PersonFriendsResult(
                    2,
                    "person2",
                    "two-\u16a0\u3055\u4e35\u05e4\u0634",
                    0
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            assertThat( rows.hasNext(), equalTo( false ) );

            personId = 4;
            operation = new LdbcShortQuery3PersonFriends( personId );
            rows = neo4jShortQuery3Impl( connection, operation ).iterator();
            assertThat( rows.hasNext(), equalTo( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void shortQuery4ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.ShortQuery4GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long messageId;
            LdbcShortQuery4MessageContent operation;

            LdbcShortQuery4MessageContentResult actualResult;
            LdbcShortQuery4MessageContentResult expectedRow;

            messageId = 0;
            operation = new LdbcShortQuery4MessageContent( messageId );
            actualResult = neo4jShortQuery4Impl( connection, operation );
            expectedRow = new LdbcShortQuery4MessageContentResult( "post_content0", 0 );
            assertThat( actualResult, equalTo( expectedRow ) );

            messageId = 1;
            operation = new LdbcShortQuery4MessageContent( messageId );
            actualResult = neo4jShortQuery4Impl( connection, operation );
            expectedRow = new LdbcShortQuery4MessageContentResult( "post_image1", 1 );
            assertThat( actualResult, equalTo( expectedRow ) );

            messageId = 2;
            operation = new LdbcShortQuery4MessageContent( messageId );
            actualResult = neo4jShortQuery4Impl( connection, operation );
            expectedRow = new LdbcShortQuery4MessageContentResult( "comment_content2", 2 );
            assertThat( actualResult, equalTo( expectedRow ) );

            messageId = 3;
            operation = new LdbcShortQuery4MessageContent( messageId );
            actualResult = neo4jShortQuery4Impl( connection, operation );
            expectedRow = new LdbcShortQuery4MessageContentResult( "comment_content3", 3 );
            assertThat( actualResult, equalTo( expectedRow ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void shortQuery5ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.ShortQuery5GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long messageId;
            LdbcShortQuery5MessageCreator operation;

            LdbcShortQuery5MessageCreatorResult actualResult;
            LdbcShortQuery5MessageCreatorResult expectedRow;

            messageId = 0;
            operation = new LdbcShortQuery5MessageCreator( messageId );
            actualResult = neo4jShortQuery5Impl( connection, operation );
            expectedRow = new LdbcShortQuery5MessageCreatorResult(
                    0,
                    "person0",
                    "zero-\u16a0\u3055\u4e35\u05e4\u0634"
            );
            assertThat( actualResult, equalTo( expectedRow ) );

            messageId = 1;
            operation = new LdbcShortQuery5MessageCreator( messageId );
            actualResult = neo4jShortQuery5Impl( connection, operation );
            expectedRow = new LdbcShortQuery5MessageCreatorResult(
                    1,
                    "person1",
                    "one-\u16a0\u3055\u4e35\u05e4\u0634"
            );
            assertThat( actualResult, equalTo( expectedRow ) );

            messageId = 2;
            operation = new LdbcShortQuery5MessageCreator( messageId );
            actualResult = neo4jShortQuery5Impl( connection, operation );
            expectedRow = new LdbcShortQuery5MessageCreatorResult(
                    0,
                    "person0",
                    "zero-\u16a0\u3055\u4e35\u05e4\u0634"
            );
            assertThat( actualResult, equalTo( expectedRow ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void shortQuery6ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.ShortQuery6GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR,
                                                     configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long messageId;
            LdbcShortQuery6MessageForum operation;

            LdbcShortQuery6MessageForumResult actualResult;
            LdbcShortQuery6MessageForumResult expectedRow;
            long forumId;
            String forumTitle;
            long moderatorId;
            String moderatorFirstName;
            String moderatorLastName;

            messageId = 0;
            operation = new LdbcShortQuery6MessageForum( messageId );
            actualResult = neo4jShortQuery6Impl( connection, operation );
            forumId = 0;
            forumTitle = "forum0";
            moderatorId = 0;
            moderatorFirstName = "person0";
            moderatorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery6MessageForumResult(
                    forumId,
                    forumTitle,
                    moderatorId,
                    moderatorFirstName,
                    moderatorLastName
            );
            assertThat( actualResult, equalTo( expectedRow ) );

            messageId = 10;
            operation = new LdbcShortQuery6MessageForum( messageId );
            actualResult = neo4jShortQuery6Impl( connection, operation );
            forumId = 0;
            forumTitle = "forum0";
            moderatorId = 0;
            moderatorFirstName = "person0";
            moderatorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery6MessageForumResult(
                    forumId,
                    forumTitle,
                    moderatorId,
                    moderatorFirstName,
                    moderatorLastName
            );
            assertThat( actualResult, equalTo( expectedRow ) );

            messageId = 11;
            operation = new LdbcShortQuery6MessageForum( messageId );
            actualResult = neo4jShortQuery6Impl( connection, operation );
            forumId = 0;
            forumTitle = "forum0";
            moderatorId = 0;
            moderatorFirstName = "person0";
            moderatorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery6MessageForumResult(
                    forumId,
                    forumTitle,
                    moderatorId,
                    moderatorFirstName,
                    moderatorLastName
            );
            assertThat( actualResult, equalTo( expectedRow ) );

            messageId = 12;
            operation = new LdbcShortQuery6MessageForum( messageId );
            actualResult = neo4jShortQuery6Impl( connection, operation );
            forumId = 0;
            forumTitle = "forum0";
            moderatorId = 0;
            moderatorFirstName = "person0";
            moderatorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery6MessageForumResult(
                    forumId,
                    forumTitle,
                    moderatorId,
                    moderatorFirstName,
                    moderatorLastName
            );
            assertThat( actualResult, equalTo( expectedRow ) );

            messageId = 13;
            operation = new LdbcShortQuery6MessageForum( messageId );
            actualResult = neo4jShortQuery6Impl( connection, operation );
            forumId = 0;
            forumTitle = "forum0";
            moderatorId = 0;
            moderatorFirstName = "person0";
            moderatorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery6MessageForumResult(
                    forumId,
                    forumTitle,
                    moderatorId,
                    moderatorFirstName,
                    moderatorLastName
            );
            assertThat( actualResult, equalTo( expectedRow ) );

            messageId = 14;
            operation = new LdbcShortQuery6MessageForum( messageId );
            actualResult = neo4jShortQuery6Impl( connection, operation );
            forumId = 0;
            forumTitle = "forum0";
            moderatorId = 0;
            moderatorFirstName = "person0";
            moderatorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery6MessageForumResult(
                    forumId,
                    forumTitle,
                    moderatorId,
                    moderatorFirstName,
                    moderatorLastName
            );
            assertThat( actualResult, equalTo( expectedRow ) );

            messageId = 1;
            operation = new LdbcShortQuery6MessageForum( messageId );
            actualResult = neo4jShortQuery6Impl( connection, operation );
            forumId = 0;
            forumTitle = "forum0";
            moderatorId = 0;
            moderatorFirstName = "person0";
            moderatorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery6MessageForumResult(
                    forumId,
                    forumTitle,
                    moderatorId,
                    moderatorFirstName,
                    moderatorLastName
            );
            assertThat( actualResult, equalTo( expectedRow ) );

            long postId = 2;
            String imageFile = "imageFile2";
            Date creationDate = new Date( 2 );
            String locationIp = "ip2";
            String browserUsed = "browser2";
            String language = "language2";
            String content = "";
            int length = 0;
            long authorPersonId = 1;
            forumId = 0;
            long countryId = 0;
            List<Long> tagIds = new ArrayList<>();
            LdbcUpdate6AddPost addPost = new LdbcUpdate6AddPost(
                    postId,
                    imageFile,
                    creationDate,
                    locationIp,
                    browserUsed,
                    language,
                    content,
                    length,
                    authorPersonId,
                    forumId,
                    countryId,
                    tagIds
            );
            neo4jUpdate6Impl( connection, addPost );

            messageId = 2;
            operation = new LdbcShortQuery6MessageForum( messageId );
            actualResult = neo4jShortQuery6Impl( connection, operation );
            forumId = 0;
            forumTitle = "forum0";
            moderatorId = 0;
            moderatorFirstName = "person0";
            moderatorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery6MessageForumResult(
                    forumId,
                    forumTitle,
                    moderatorId,
                    moderatorFirstName,
                    moderatorLastName
            );
            assertThat( actualResult, equalTo( expectedRow ) );

            long commentId = 15;
            creationDate = new Date( 15 );
            locationIp = "ip15";
            browserUsed = "browser15";
            content = "comment15";
            length = content.length();
            authorPersonId = 0;
            countryId = 0;
            long replyToPostId = 2;
            long replyToCommentId = -1;
            tagIds = new ArrayList<>();
            LdbcUpdate7AddComment addComment = new LdbcUpdate7AddComment(
                    commentId,
                    creationDate,
                    locationIp,
                    browserUsed,
                    content,
                    length,
                    authorPersonId,
                    countryId,
                    replyToPostId,
                    replyToCommentId,
                    tagIds
            );
            neo4jUpdate7Impl( connection, addComment );

            messageId = 15;
            operation = new LdbcShortQuery6MessageForum( messageId );
            actualResult = neo4jShortQuery6Impl( connection, operation );
            forumId = 0;
            forumTitle = "forum0";
            moderatorId = 0;
            moderatorFirstName = "person0";
            moderatorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            expectedRow = new LdbcShortQuery6MessageForumResult(
                    forumId,
                    forumTitle,
                    moderatorId,
                    moderatorFirstName,
                    moderatorLastName
            );
            assertThat( actualResult, equalTo( expectedRow ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }

    @Test
    public void shortQuery7ShouldReturnExpectedResult() throws Exception
    {
        File dbDir = testFolder.directory( "db" ).toFile();
        File configDir = testFolder.directory( "config" ).toFile();
        QueryGraphMaker.createDbFromQueryGraphMaker( new SnbInteractiveTestGraph.ShortQuery7GraphMaker(),
                                                     dbDir,
                                                     Neo4jSchema.NEO4J_REGULAR, configDir );
        CONNECTION connection = openConnection( dbDir, configDir );
        try
        {
            long messageId;
            LdbcShortQuery7MessageReplies operation;

            Iterator<LdbcShortQuery7MessageRepliesResult> rows;
            LdbcShortQuery7MessageRepliesResult expectedRow;
            long commentId;
            String commentContent;
            long commentCreationDate;
            long replyAuthorId;
            String replyAuthorFirstName;
            String replyAuthorLastName;
            boolean replayAuthorKnowsOriginalMessageAuthor;

            messageId = 0;
            operation = new LdbcShortQuery7MessageReplies( messageId );
            rows = neo4jShortQuery7Impl( connection, operation ).iterator();
            assertThat( rows.hasNext(), is( false ) );

            messageId = 1;
            operation = new LdbcShortQuery7MessageReplies( messageId );
            rows = neo4jShortQuery7Impl( connection, operation ).iterator();
            commentId = 3;
            commentContent = "comment_content3";
            commentCreationDate = 3;
            replyAuthorId = 0;
            replyAuthorFirstName = "person0";
            replyAuthorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            replayAuthorKnowsOriginalMessageAuthor = false;
            expectedRow = new LdbcShortQuery7MessageRepliesResult(
                    commentId,
                    commentContent,
                    commentCreationDate,
                    replyAuthorId,
                    replyAuthorFirstName,
                    replyAuthorLastName,
                    replayAuthorKnowsOriginalMessageAuthor
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            commentId = 5;
            commentContent = "comment_content5";
            commentCreationDate = 3;
            replyAuthorId = 2;
            replyAuthorFirstName = "person2";
            replyAuthorLastName = "two-\u16a0\u3055\u4e35\u05e4\u0634";
            replayAuthorKnowsOriginalMessageAuthor = true;
            expectedRow = new LdbcShortQuery7MessageRepliesResult(
                    commentId,
                    commentContent,
                    commentCreationDate,
                    replyAuthorId,
                    replyAuthorFirstName,
                    replyAuthorLastName,
                    replayAuthorKnowsOriginalMessageAuthor
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            commentId = 2;
            commentContent = "comment_content2";
            commentCreationDate = 2;
            replyAuthorId = 0;
            replyAuthorFirstName = "person0";
            replyAuthorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            replayAuthorKnowsOriginalMessageAuthor = false;
            expectedRow = new LdbcShortQuery7MessageRepliesResult(
                    commentId,
                    commentContent,
                    commentCreationDate,
                    replyAuthorId,
                    replyAuthorFirstName,
                    replyAuthorLastName,
                    replayAuthorKnowsOriginalMessageAuthor
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            assertThat( rows.hasNext(), equalTo( false ) );

            messageId = 2;
            operation = new LdbcShortQuery7MessageReplies( messageId );
            rows = neo4jShortQuery7Impl( connection, operation ).iterator();
            commentId = 4;
            commentContent = "comment_content4";
            commentCreationDate = 4;
            replyAuthorId = 0;
            replyAuthorFirstName = "person0";
            replyAuthorLastName = "zero-\u16a0\u3055\u4e35\u05e4\u0634";
            replayAuthorKnowsOriginalMessageAuthor = false;
            expectedRow = new LdbcShortQuery7MessageRepliesResult(
                    commentId,
                    commentContent,
                    commentCreationDate,
                    replyAuthorId,
                    replyAuthorFirstName,
                    replyAuthorLastName,
                    replayAuthorKnowsOriginalMessageAuthor
            );
            assertThat( rows.next(), equalTo( expectedRow ) );
            assertThat( rows.hasNext(), is( false ) );

            messageId = 3;
            operation = new LdbcShortQuery7MessageReplies( messageId );
            rows = neo4jShortQuery7Impl( connection, operation ).iterator();
            assertThat( rows.hasNext(), is( false ) );

            messageId = 4;
            operation = new LdbcShortQuery7MessageReplies( messageId );
            rows = neo4jShortQuery7Impl( connection, operation ).iterator();
            assertThat( rows.hasNext(), is( false ) );

            messageId = 5;
            operation = new LdbcShortQuery7MessageReplies( messageId );
            rows = neo4jShortQuery7Impl( connection, operation ).iterator();
            assertThat( rows.hasNext(), is( false ) );
        }
        finally
        {
            closeConnection( connection );
        }
    }
}
