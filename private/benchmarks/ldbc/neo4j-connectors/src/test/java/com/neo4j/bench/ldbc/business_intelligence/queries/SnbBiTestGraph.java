/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries;

import com.neo4j.bench.ldbc.Domain.Forum;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Place;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.Domain.Tag;
import com.neo4j.bench.ldbc.Domain.TagClass;
import com.neo4j.bench.ldbc.QueryGraphMaker;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.internal.helpers.collection.MapUtil;

public class SnbBiTestGraph
{
    public static class Query1GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Forums
                   + " (forum0:" + Nodes.Forum + "),\n"
                   + " (forum1:" + Nodes.Forum + "),\n"
                   // Posts
                   + " (post0:" + Nodes.Message + ":" + Nodes.Post + " $post0),\n"
                   + " (post1:" + Nodes.Message + ":" + Nodes.Post + " $post1),\n"
                   + " (post2:" + Nodes.Message + ":" + Nodes.Post + " $post2),\n"
                   // Comments
                   + " (comment0:" + Nodes.Message + ":" + Nodes.Comment + " $comment0),\n"
                   + " (comment1:" + Nodes.Message + ":" + Nodes.Comment + " $comment1),\n"
                   + " (comment2:" + Nodes.Message + ":" + Nodes.Comment + " $comment2),\n"
                   + " (comment3:" + Nodes.Message + ":" + Nodes.Comment + " $comment3),\n"
                   + " (comment4:" + Nodes.Message + ":" + Nodes.Comment + " $comment4),\n"
                   + " (comment5:" + Nodes.Message + ":" + Nodes.Comment + " $comment5),\n"
                   + " (comment6:" + Nodes.Message + ":" + Nodes.Comment + " $comment6),\n"
                   + " (comment7:" + Nodes.Message + ":" + Nodes.Comment + " $comment7),\n"
                   + " (comment8:" + Nodes.Message + ":" + Nodes.Comment + " $comment8),\n"
                   + " (comment9:" + Nodes.Message + ":" + Nodes.Comment + " $comment9),\n"
                   + " (comment10:" + Nodes.Message + ":" + Nodes.Comment + " $comment10),\n"
                   + " (comment11:" + Nodes.Message + ":" + Nodes.Comment + " $comment11),\n"
                   + " (comment12:" + Nodes.Message + ":" + Nodes.Comment + " $comment12),\n"
                   + " (comment13:" + Nodes.Message + ":" + Nodes.Comment + " $comment13),\n"
                   + " (comment14:" + Nodes.Message + ":" + Nodes.Comment + " $comment14),\n"
                   + " (comment15:" + Nodes.Message + ":" + Nodes.Comment + " $comment15),\n"
                   + " (comment16:" + Nodes.Message + ":" + Nodes.Comment + " $comment16),\n"
                   + " (comment17:" + Nodes.Message + ":" + Nodes.Comment + " $comment17),\n"
                   + " (comment18:" + Nodes.Message + ":" + Nodes.Comment + " $comment18),\n"
                   + " (comment19:" + Nodes.Message + ":" + Nodes.Comment + " $comment19),\n"
                   + " (comment20:" + Nodes.Message + ":" + Nodes.Comment + " $comment20),\n"

                   // Post Forum
                   + " (forum0)-[:" + Rels.CONTAINER_OF + "]->(post0),\n"
                   + " (forum0)-[:" + Rels.CONTAINER_OF + "]->(post1),\n"
                   + " (forum1)-[:" + Rels.CONTAINER_OF + "]->(post2),\n"
                   // Post Comment
                   + " (comment0)-[:" + Rels.REPLY_OF_POST + "]->(post0),\n"
                   + " (comment1)-[:" + Rels.REPLY_OF_POST + "]->(post0),\n"
                   + " (comment2)-[:" + Rels.REPLY_OF_POST + "]->(post1),\n"
                   + " (comment3)-[:" + Rels.REPLY_OF_POST + "]->(post2),\n"
                   // Comment Comment
                   + " (comment4)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment0),\n"
                   + " (comment5)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment4),\n"
                   + " (comment6)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment5),\n"
                   + " (comment7)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment0),\n"
                   + " (comment8)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment7),\n"
                   + " (comment9)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment8),\n"
                   + " (comment10)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment5),\n"
                   + " (comment11)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment3),\n"
                   + " (comment12)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment11),\n"
                   + " (comment13)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment12),\n"
                   + " (comment14)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment3),\n"
                   + " (comment15)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment2),\n"
                   + " (comment16)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment15),\n"
                   + " (comment17)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment15),\n"
                   + " (comment18)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment17),\n"
                   + " (comment19)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment17),\n"
                   + " (comment20)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment17)";
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    "post0",
                    MapUtil.map( Message.ID, 0L, Message.LENGTH, 30, Message.CREATION_DATE, date( 2009 ) ),
                    "post1",
                    MapUtil.map( Message.ID, 1L, Message.LENGTH, 40, Message.CREATION_DATE, date( 2010 ) ),
                    "post2",
                    MapUtil.map( Message.ID, 2L, Message.LENGTH, 80, Message.CREATION_DATE, date( 2009 ) ),
                    "comment0",
                    MapUtil.map( Message.ID, 100L, Message.LENGTH, 160, Message.CREATION_DATE, date( 2010 ) ),
                    "comment1",
                    MapUtil.map( Message.ID, 101L, Message.LENGTH, 39, Message.CREATION_DATE, date( 2011 ) ),
                    "comment2",
                    MapUtil.map( Message.ID, 102L, Message.LENGTH, 41, Message.CREATION_DATE, date( 2009 ) ),
                    "comment3",
                    MapUtil.map( Message.ID, 103L, Message.LENGTH, 81, Message.CREATION_DATE, date( 2009 ) ),
                    "comment4",
                    MapUtil.map( Message.ID, 104L, Message.LENGTH, 161, Message.CREATION_DATE, date( 2011 ) ),
                    "comment5",
                    MapUtil.map( Message.ID, 105L, Message.LENGTH, 10, Message.CREATION_DATE, date( 2009 ) ),
                    "comment6",
                    MapUtil.map( Message.ID, 106L, Message.LENGTH, 79, Message.CREATION_DATE, date( 2010 ) ),
                    "comment7",
                    MapUtil.map( Message.ID, 107L, Message.LENGTH, 22, Message.CREATION_DATE, date( 2011 ) ),
                    "comment8",
                    MapUtil.map( Message.ID, 108L, Message.LENGTH, 60, Message.CREATION_DATE, date( 2009 ) ),
                    "comment9",
                    MapUtil.map( Message.ID, 109L, Message.LENGTH, 93, Message.CREATION_DATE, date( 2010 ) ),
                    "comment10",
                    MapUtil.map( Message.ID, 110L, Message.LENGTH, 160, Message.CREATION_DATE, date( 2011 ) ),
                    "comment11",
                    MapUtil.map( Message.ID, 111L, Message.LENGTH, 200, Message.CREATION_DATE, date( 2011 ) ),
                    "comment12",
                    MapUtil.map( Message.ID, 112L, Message.LENGTH, 7, Message.CREATION_DATE, date( 2011 ) ),
                    "comment13",
                    MapUtil.map( Message.ID, 113L, Message.LENGTH, 159, Message.CREATION_DATE, date( 2009 ) ),
                    "comment14",
                    MapUtil.map( Message.ID, 114L, Message.LENGTH, 150, Message.CREATION_DATE, date( 2010 ) ),
                    "comment15",
                    MapUtil.map( Message.ID, 115L, Message.LENGTH, 55, Message.CREATION_DATE, date( 2011 ) ),
                    "comment16",
                    MapUtil.map( Message.ID, 116L, Message.LENGTH, 112, Message.CREATION_DATE, date( 2009 ) ),
                    "comment17",
                    MapUtil.map( Message.ID, 117L, Message.LENGTH, 187, Message.CREATION_DATE, date( 2010 ) ),
                    "comment18",
                    MapUtil.map( Message.ID, 118L, Message.LENGTH, 199, Message.CREATION_DATE, date( 2011 ) ),
                    "comment19",
                    MapUtil.map( Message.ID, 119L, Message.LENGTH, 19, Message.CREATION_DATE, date( 2009 ) ),
                    "comment20",
                    MapUtil.map( Message.ID, 120L, Message.LENGTH, 62, Message.CREATION_DATE, date( 2010 ) )
            );
        }
    }

    public static class Query2GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return
                    "WITH "
                    + date( 2011, 1 ) + " AS month1, "
                    + date( 2010, 3 ) + " AS month3, "
                    + date( 2001, 4 ) + " AS month4, "
                    + date( 2002, 5 ) + " AS month5, "
                    + date( 1989, 12 ) + " AS month12\n"
                    // Messages from Month 1
                    + "FOREACH (id IN range(1,1184,1) | CREATE (:" + Nodes.Message + ":" + Nodes.Post
                    + " {" + Message.CREATION_DATE + ":month1}) )\n"

                    // Messages from Month 3
                    + "FOREACH (id IN range(1,200,1) | CREATE (:" + Nodes.Message + ":" + Nodes.Comment
                    + " {" + Message.CREATION_DATE + ":month3}) )\n"

                    // Messages from Month 4
                    + "FOREACH (id IN range(1,192,1) | CREATE (:" + Nodes.Message + ":" + Nodes.Comment
                    + " {" + Message.CREATION_DATE + ":month4}) )\n"

                    // Messages from Month 5
                    + "FOREACH (id IN range(1,175,1) | CREATE (:" + Nodes.Message + ":" + Nodes.Comment
                    + " {" + Message.CREATION_DATE + ":month5}) )\n"

                    // Messages from Month 12
                    + "FOREACH (id IN range(1,1,1) | CREATE (:" + Nodes.Message + ":" + Nodes.Comment
                    + " {" + Message.CREATION_DATE + ":month12}) )\n"

                    + "WITH month1, month3, month4, month5, month12\n"

                    + "MATCH (month1Message:" + Nodes.Message + " {" + Message.CREATION_DATE + ":month1})\n"
                    + "WITH"
                    + " collect(month1Message) AS month1Messages,"
                    + " month3, month4, month5, month12\n"
                    + "MATCH (month3Message:" + Nodes.Message + " {" + Message.CREATION_DATE + ":month3})\n"
                    + "WITH"
                    + " month1Messages,"
                    + " collect(month3Message) AS month3Messages,"
                    + " month4, month5, month12\n"
                    + "MATCH (month4Message:" + Nodes.Message + " {" + Message.CREATION_DATE + ":month4})\n"
                    + "WITH"
                    + " month1Messages,"
                    + " month3Messages,"
                    + " collect(month4Message) AS month4Messages,"
                    + " month5, month12\n"
                    + "MATCH (month5Message:" + Nodes.Message + " {" + Message.CREATION_DATE + ":month5})\n"
                    + "WITH"
                    + " month1Messages,"
                    + " month3Messages,"
                    + " month4Messages,"
                    + " collect(month5Message) AS month5Messages,"
                    + " month12\n"
                    + "MATCH (month12Message:" + Nodes.Message + " {" + Message.CREATION_DATE + ":month12})\n"
                    + "WITH"
                    + " month1Messages,"
                    + " month3Messages,"
                    + " month4Messages,"
                    + " month5Messages,"
                    + " collect(month12Message) AS month12Messages, "
                    + (date( 2013, 1, 1 ) - duration( 4 )) + " AS age4, "
                    + (date( 2013, 1, 1 ) - duration( 15 )) + " AS age15, "
                    + (date( 2013, 1, 1 ) - duration( 20 )) + " AS age20, "
                    + (date( 2013, 1, 1 ) - duration( 27 )) + " AS age27, "
                    + (date( 2013, 1, 1 ) - duration( 30 )) + " AS age30, "
                    + (date( 2013, 1, 1 ) - duration( 36 )) + " AS age36, "
                    + (date( 2013, 1, 1 ) - duration( 40 )) + " AS age40\n"

                    + "CREATE\n"

                    + " (country_a:" + Place.Type.Country + " {name:'countryA'}),\n"
                    + " (country_b:" + Place.Type.Country + " {name:'countryB'}),\n"
                    + " (country_c:" + Place.Type.Country + " {name:'countryC'}),\n"

                    + " (city_country_a:" + Place.Type.City + " {name:'cityA'}),\n"
                    + " (city_country_b:" + Place.Type.City + " {name:'cityB'}),\n"
                    + " (city_country_c:" + Place.Type.City + " {name:'cityC'}),\n"

                    + " (person_male_20_a:" + Nodes.Person
                    + " {" + Person.GENDER + ":'male', " + Person.BIRTHDAY + ":age20}),\n"
                    + " (person_male_20_b:" + Nodes.Person
                    + " {" + Person.GENDER + ":'male', " + Person.BIRTHDAY + ":age20}),\n"
                    + " (person_male_27_a:" + Nodes.Person
                    + " {" + Person.GENDER + ":'male', " + Person.BIRTHDAY + ":age27}),\n"
                    + " (person_male_30_a:" + Nodes.Person
                    + " {" + Person.GENDER + ":'male', " + Person.BIRTHDAY + ":age30}),\n"
                    + " (person_male_30_b:" + Nodes.Person
                    + " {" + Person.GENDER + ":'male', " + Person.BIRTHDAY + ":age30}),\n"
                    + " (person_male_40_a:" + Nodes.Person
                    + " {" + Person.GENDER + ":'male', " + Person.BIRTHDAY + ":age40}),\n"
                    + " (person_female_4_a:" + Nodes.Person
                    + " {" + Person.GENDER + ":'female', " + Person.BIRTHDAY + ":age4}),\n"
                    + " (person_female_15_a:" + Nodes.Person
                    + " {" + Person.GENDER + ":'female', " + Person.BIRTHDAY + ":age15}),\n"
                    + " (person_female_20_a:" + Nodes.Person
                    + " {" + Person.GENDER + ":'female', " + Person.BIRTHDAY + ":age20}),\n"
                    + " (person_female_30_a:" + Nodes.Person
                    + " {" + Person.GENDER + ":'female', " + Person.BIRTHDAY + ":age30}),\n"
                    + " (person_female_36_a:" + Nodes.Person
                    + " {" + Person.GENDER + ":'female', " + Person.BIRTHDAY + ":age36}),\n"

                    + " (tag_a:" + Nodes.Tag + " {name:'tagA'}),\n"
                    + " (tag_b:" + Nodes.Tag + " {name:'tagB'}),\n"
                    + " (tag_c:" + Nodes.Tag + " {name:'tagC'}),\n"
                    + " (tag_d:" + Nodes.Tag + " {name:'tagD'}),\n"
                    + " (tag_e:" + Nodes.Tag + " {name:'tagE'}),\n"

                    + " (city_country_a)-[:" + Rels.IS_PART_OF + "]->(country_a),\n"
                    + " (city_country_b)-[:" + Rels.IS_PART_OF + "]->(country_b),\n"
                    + " (city_country_c)-[:" + Rels.IS_PART_OF + "]->(country_c),\n"

                    + " (city_country_a)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person_male_20_a),\n"
                    + " (city_country_a)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person_male_20_b),\n"
                    + " (city_country_a)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person_male_30_a),\n"
                    + " (city_country_a)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person_male_30_b),\n"
                    + " (city_country_a)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person_male_40_a),\n"
                    + " (city_country_a)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person_female_20_a),\n"
                    + " (city_country_a)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person_female_36_a),\n"
                    + " (city_country_a)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person_female_4_a),\n"
                    + " (city_country_a)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person_female_15_a),\n"
                    + " (city_country_b)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person_male_27_a),\n"
                    + " (city_country_c)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person_female_30_a)\n"

                    // ---------------
                    // --- MONTH 1 ---
                    // ---------------
                    // country_a -- month_1 -- male -- age_20 -- tag_a : 20 messages
                    + "FOREACH (month1Message IN [i IN range(0,19,1) | month1Messages[i-1]] |"
                    + " CREATE (person_male_20_a)<-[:" + Rels.POST_HAS_CREATOR + "]-(month1Message)"
                    + "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_a))\n"
                    // country_a -- month_1 -- male -- age_30 -- tag_a : 100 messages
                    + "FOREACH (month1Message IN [i IN range(20,119,1) | month1Messages[i-1]] |"
                    + " CREATE (person_male_30_a)<-[:" + Rels.POST_HAS_CREATOR + "]-(month1Message)"
                    + "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_a))\n"
                    // country_a -- month_1 -- male -- age_30 -- tag_a : 2 messages
                    + "FOREACH (month1Message IN [i IN range(120,121,1) | month1Messages[i-1]] |"
                    + " CREATE (person_male_30_b)<-[:" + Rels.POST_HAS_CREATOR + "]-(month1Message)"
                    + "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_a))\n"
                    // country_a -- month_1 -- female -- age_20 -- tag_a : 120 messages
                    + "FOREACH (month1Message IN [i IN range(122,241,1) | month1Messages[i-1]] |"
                    + " CREATE (person_female_20_a)<-[:" + Rels.POST_HAS_CREATOR + "]-(month1Message)"
                    + "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_a))\n"

                    // country_a -- month_1 -- male -- age_20 -- tag_b : 101 messages
                    + "FOREACH (month1Message IN [i IN range(333,433,1) | month1Messages[i-1]] |"
                    + " CREATE (person_male_20_b)<-[:" + Rels.POST_HAS_CREATOR + "]-(month1Message)" +
                    "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_b))\n"
                    // country_a -- month_1 -- male -- age_40 -- tag_b : 200 messages
                    + "FOREACH (month1Message IN [i IN range(434,633,1) | month1Messages[i-1]] |"
                    + " CREATE (person_male_40_a)<-[:" + Rels.POST_HAS_CREATOR + "]-(month1Message)" +
                    "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_b))\n"

                    // country_a -- month_1 -- male -- age_40 -- tag_c : 100 messages
                    + "FOREACH (month1Message IN [i IN range(634,733,1) | month1Messages[i-1]] |"
                    + " CREATE (person_male_40_a)<-[:" + Rels.POST_HAS_CREATOR + "]-(month1Message)" +
                    "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_c))\n"
                    // country_a -- month_1 -- female -- age_36 -- tag_c : 300 messages
                    + "FOREACH (month1Message IN [i IN range(734,1033,1) | month1Messages[i-1]] |"
                    + " CREATE (person_female_36_a)<-[:" + Rels.POST_HAS_CREATOR + "]-(month1Message)" +
                    "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_c))\n"

                    // country_a -- month_1 -- male -- age_40 -- tag_d : 150 messages
                    + "FOREACH (month1Message IN [i IN range(1034,1183,1) | month1Messages[i-1]] |"
                    + " CREATE (person_male_40_a)<-[:" + Rels.POST_HAS_CREATOR + "]-(month1Message)" +
                    "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_d))\n"

                    // ---------------
                    // --- MONTH 3 ---
                    // ---------------
                    // country_b -- month_3 -- male -- age_27 -- tag_a : 200 messages
                    + "FOREACH (month3Message IN [i IN range(0,199,1) | month3Messages[i-1]] |"
                    + " CREATE (person_male_27_a)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(month3Message)" +
                    "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_a))\n"

                    // ---------------
                    // --- MONTH 4 ---
                    // ---------------
                    // country_a -- month_4 -- female -- age_4 -- tag_a : 91 messages
                    + "FOREACH (month4Message IN [i IN range(0,90,1) | month4Messages[i-1]] |"
                    + " CREATE (person_female_4_a)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(month4Message)" +
                    "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_a))\n"

                    // country_a -- month_4 -- female -- age_4 -- tag_d : 100 messages
                    + "FOREACH (month4Message IN [i IN range(91,190,1) | month4Messages[i-1]] |"
                    + " CREATE (person_female_4_a)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(month4Message)" +
                    "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_d))\n"

                    // country_a -- month_4 -- female -- age_4 -- tag_e : 1 messages
                    + "FOREACH (month4Message IN [i IN range(191,191,1) | month4Messages[i-1]] |"
                    + " CREATE (person_female_4_a)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(month4Message)" +
                    "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_e))\n"

                    // ---------------
                    // --- MONTH 5 ---
                    // ---------------
                    // country_a -- month_5 -- female -- age_15 -- tag_a : 175 messages
                    + "FOREACH (month5Message IN [i IN range(0,174,1) | month5Messages[i-1]] |"
                    + " CREATE (person_female_15_a)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(month5Message)" +
                    "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_a))\n"

                    // ---------------
                    // --- MONTH 12 ---
                    // ---------------
                    // country_c -- month_12 -- female -- age_30 -- tag_b : 1 messages
                    + "FOREACH (month12Message IN [i IN range(0,0,1) | month12Messages[i-1]] |"
                    + " CREATE (person_female_30_a)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(month12Message)" +
                    "-[:" + Rels.COMMENT_HAS_TAG + "]->(tag_b))\n"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query3GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Comments
                   + " (comment_2009_1:" + Nodes.Message + ":" + Nodes.Comment
                   + "{" + Message.CREATION_DATE + ":" + date( 2009, 1 ) + "}),\n"
                   + " (comment_2009_3:" + Nodes.Message + ":" + Nodes.Comment
                   + "{" + Message.CREATION_DATE + ":" + date( 2009, 3 ) + "}),\n"
                   + " (comment_2010_1:" + Nodes.Message + ":" + Nodes.Comment
                   + "{" + Message.CREATION_DATE + ":" + date( 2010, 1 ) + "}),\n"
                   + " (comment_2010_2_a:" + Nodes.Message + ":" + Nodes.Comment
                   + "{" + Message.CREATION_DATE + ":" + date( 2010, 2 ) + "}),\n"

                   // Posts
                   + " (post_2010_2_b:" + Nodes.Message + ":" + Nodes.Post
                   + "{" + Message.CREATION_DATE + ":" + date( 2010, 2 ) + "}),\n"
                   + " (post_2010_2_c:" + Nodes.Message + ":" + Nodes.Post
                   + "{" + Message.CREATION_DATE + ":" + date( 2010, 2 ) + "}),\n"
                   + " (post_2010_3_a:" + Nodes.Message + ":" + Nodes.Post
                   + "{" + Message.CREATION_DATE + ":" + date( 2010, 3 ) + "}),\n"
                   + " (post_2010_3_b:" + Nodes.Message + ":" + Nodes.Post
                   + "{" + Message.CREATION_DATE + ":" + date( 2010, 3 ) + "}),\n"
                   + " (post_2010_3_c:" + Nodes.Message + ":" + Nodes.Post
                   + "{" + Message.CREATION_DATE + ":" + date( 2010, 3 ) + "}),\n"
                   + " (post_2010_4:" + Nodes.Message + ":" + Nodes.Post
                   + "{" + Message.CREATION_DATE + ":" + date( 2010, 4 ) + "}),\n"
                   + " (post_2011_1:" + Nodes.Message + ":" + Nodes.Post
                   + "{" + Message.CREATION_DATE + ":" + date( 2011, 1 ) + "}),\n"
                   + " (post_2011_2:" + Nodes.Message + ":" + Nodes.Post
                   + "{" + Message.CREATION_DATE + ":" + date( 2011, 2 ) + "}),\n"
                   + " (post_2011_3:" + Nodes.Message + ":" + Nodes.Post
                   + "{" + Message.CREATION_DATE + ":" + date( 2011, 3 ) + "}),\n"
                   + " (post_2011_4:" + Nodes.Message + ":" + Nodes.Post
                   + "{" + Message.CREATION_DATE + ":" + date( 2011, 4 ) + "}),\n"

                   // Tags
                   + " (tag1:" + Nodes.Tag + "{" + Tag.NAME + ":'Tag1'}),\n"
                   + " (tag2:" + Nodes.Tag + "{" + Tag.NAME + ":'Tag2'}),\n"
                   + " (tag3:" + Nodes.Tag + "{" + Tag.NAME + ":'Tag3'}),\n"
                   + " (tag4:" + Nodes.Tag + "{" + Tag.NAME + ":'Tag4'}),\n"

                   // Post Tag
                   + " (post_2010_2_b)-[:" + Rels.POST_HAS_TAG + "]->(tag3),\n"
                   + " (post_2010_2_c)-[:" + Rels.POST_HAS_TAG + "]->(tag3),\n"
                   + " (post_2010_3_a)-[:" + Rels.POST_HAS_TAG + "]->(tag2),\n"
                   + " (post_2010_3_b)-[:" + Rels.POST_HAS_TAG + "]->(tag1),\n"
                   + " (post_2010_3_b)-[:" + Rels.POST_HAS_TAG + "]->(tag4),\n"
                   + " (post_2010_3_c)-[:" + Rels.POST_HAS_TAG + "]->(tag4),\n"
                   + " (post_2010_4)-[:" + Rels.POST_HAS_TAG + "]->(tag4),\n"
                   + " (post_2011_1)-[:" + Rels.POST_HAS_TAG + "]->(tag2),\n"
                   + " (post_2011_1)-[:" + Rels.POST_HAS_TAG + "]->(tag3),\n"
                   + " (post_2011_3)-[:" + Rels.POST_HAS_TAG + "]->(tag1),\n"
                   + " (post_2011_4)-[:" + Rels.POST_HAS_TAG + "]->(tag3),\n"
                   + " (post_2011_4)-[:" + Rels.POST_HAS_TAG + "]->(tag4),\n"

                   // Comment Tag
                   + " (comment_2009_1)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag1),\n"
                   + " (comment_2009_3)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag1),\n"
                   + " (comment_2010_1)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag2),\n"
                   + " (comment_2010_2_a)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag1),\n"
                   + " (comment_2010_2_a)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag3)\n"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query4GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Tag Class
                   + " (tagClassA:" + Nodes.TagClass + "{" + TagClass.NAME + ":'TagClassA'}),\n"
                   + " (tagClassAA:" + Nodes.TagClass + "{" + TagClass.NAME + ":'TagClassAA'}),\n"
                   + " (tagClassB:" + Nodes.TagClass + "{" + TagClass.NAME + ":'TagClassB'}),\n"
                   // Tag
                   + " (tagA1:" + Nodes.Tag + "{" + Tag.NAME + ":'TagA1'}),\n"
                   + " (tagA2:" + Nodes.Tag + "{" + Tag.NAME + ":'TagA2'}),\n"
                   + " (tagA3:" + Nodes.Tag + "{" + Tag.NAME + ":'TagA3'}),\n"
                   + " (tagAA1:" + Nodes.Tag + "{" + Tag.NAME + ":'TagAA1'}),\n"
                   + " (tagB1:" + Nodes.Tag + "{" + Tag.NAME + ":'TagB1'}),\n"
                   // Comment
                   + " (commentA1:" + Nodes.Message + Nodes.Comment + "),\n"
                   // Post
                   + " (postA1:" + Nodes.Message + Nodes.Post + "),\n"
                   + " (postA2:" + Nodes.Message + Nodes.Post + "),\n"
                   + " (postA3:" + Nodes.Message + Nodes.Post + "),\n"
                   + " (postA4:" + Nodes.Message + Nodes.Post + "),\n"
                   + " (postA5:" + Nodes.Message + Nodes.Post + "),\n"
                   + " (postA6:" + Nodes.Message + Nodes.Post + "),\n"
                   + " (postA7:" + Nodes.Message + Nodes.Post + "),\n"
                   + " (postA8:" + Nodes.Message + Nodes.Post + "),\n"
                   + " (postA9:" + Nodes.Message + Nodes.Post + "),\n"
                   + " (postA10:" + Nodes.Message + Nodes.Post + "),\n"
                   + " (postA11:" + Nodes.Message + Nodes.Post + "),\n"
                   + " (postA12:" + Nodes.Message + Nodes.Post + "),\n"
                   + " (postB1:" + Nodes.Message + Nodes.Post + "),\n"
                   + " (postB2:" + Nodes.Message + Nodes.Post + "),\n"
                   // Forum
                   + " (forumA1:" + Nodes.Forum
                   + "{" + Forum.ID + ":1," + Forum.TITLE + ":'ForumA1'," + Forum.CREATION_DATE + ":1}),\n"
                   + " (forumA2:" + Nodes.Forum
                   + "{" + Forum.ID + ":2," + Forum.TITLE + ":'ForumA2'," + Forum.CREATION_DATE + ":2}),\n"
                   + " (forumA3:" + Nodes.Forum
                   + "{" + Forum.ID + ":3," + Forum.TITLE + ":'ForumA3'," + Forum.CREATION_DATE + ":3}),\n"
                   + " (forumA4:" + Nodes.Forum
                   + "{" + Forum.ID + ":4," + Forum.TITLE + ":'ForumA4'," + Forum.CREATION_DATE + ":4}),\n"
                   + " (forumB1:" + Nodes.Forum
                   + "{" + Forum.ID + ":5," + Forum.TITLE + ":'ForumB1'," + Forum.CREATION_DATE + ":5}),\n"
                   // Person
                   + " (personA1:" + Nodes.Person + "{" + Person.ID + ":1}),\n"
                   + " (personA2:" + Nodes.Person + "{" + Person.ID + ":2}),\n"
                   + " (personA3:" + Nodes.Person + "{" + Person.ID + ":3}),\n"
                   + " (personB1:" + Nodes.Person + "{" + Person.ID + ":4}),\n"
                   // City
                   + " (cityA1:" + Place.Type.City + "),\n"
                   + " (cityA2:" + Place.Type.City + "),\n"
                   + " (cityB1:" + Place.Type.City + "),\n"
                   // Country
                   + " (countryA:" + Place.Type.Country + "{" + Place.NAME + ":'CountryA'}),\n"
                   + " (countryB:" + Place.Type.Country + "{" + Place.NAME + ":'CountryB'}),\n"

                   // TagClass TagClass
                   + " (tagClassA)<-[:" + Rels.IS_SUBCLASS_OF + "]-(tagClassAA),\n"
                   // TagClass Tag
                   + " (tagClassA)<-[:" + Rels.HAS_TYPE + "]-(tagA1),\n"
                   + " (tagClassA)<-[:" + Rels.HAS_TYPE + "]-(tagA2),\n"
                   + " (tagClassA)<-[:" + Rels.HAS_TYPE + "]-(tagA3),\n"
                   + " (tagClassAA)<-[:" + Rels.HAS_TYPE + "]-(tagAA1),\n"
                   + " (tagClassB)<-[:" + Rels.HAS_TYPE + "]-(tagB1),\n"
                   // Comment Tag
                   + " (commentA1)-[:" + Rels.COMMENT_HAS_TAG + "]->(tagA1),\n"
                   // Post Tag
                   + " (postA1)-[:" + Rels.POST_HAS_TAG + "]->(tagA1),\n"
                   + " (postA1)-[:" + Rels.POST_HAS_TAG + "]->(tagA2),\n"
                   + " (postA1)-[:" + Rels.POST_HAS_TAG + "]->(tagA3),\n"
                   + " (postA2)-[:" + Rels.POST_HAS_TAG + "]->(tagA1),\n"
                   + " (postA3)-[:" + Rels.POST_HAS_TAG + "]->(tagA2),\n"
                   + " (postA4)-[:" + Rels.POST_HAS_TAG + "]->(tagA3),\n"
                   + " (postA5)-[:" + Rels.POST_HAS_TAG + "]->(tagAA1),\n"
                   + " (postA6)-[:" + Rels.POST_HAS_TAG + "]->(tagB1),\n"
                   + " (postA7)-[:" + Rels.POST_HAS_TAG + "]->(tagA1),\n"
                   + " (postA7)-[:" + Rels.POST_HAS_TAG + "]->(tagA3),\n"
                   + " (postA8)-[:" + Rels.POST_HAS_TAG + "]->(tagA3),\n"
                   + " (postA8)-[:" + Rels.POST_HAS_TAG + "]->(tagAA1),\n"
                   + " (postA9)-[:" + Rels.POST_HAS_TAG + "]->(tagAA1),\n"
                   + " (postA9)-[:" + Rels.POST_HAS_TAG + "]->(tagB1),\n"
                   + " (postA10)-[:" + Rels.POST_HAS_TAG + "]->(tagA1),\n"
                   + " (postA11)-[:" + Rels.POST_HAS_TAG + "]->(tagA2),\n"
                   + " (postA11)-[:" + Rels.POST_HAS_TAG + "]->(tagB1),\n"
                   + " (postA12)-[:" + Rels.POST_HAS_TAG + "]->(tagAA1),\n"
                   + " (postB1)-[:" + Rels.POST_HAS_TAG + "]->(tagA3),\n"
                   + " (postB1)-[:" + Rels.POST_HAS_TAG + "]->(tagB1),\n"
                   + " (postB2)-[:" + Rels.POST_HAS_TAG + "]->(tagB1),\n"
                   // Forum Comment
                   + " (forumA1)-[:" + Rels.CONTAINER_OF + "]->(commentA1),\n"
                   // Forum Post
                   + " (forumA1)-[:" + Rels.CONTAINER_OF + "]->(postA1),\n"
                   + " (forumA1)-[:" + Rels.CONTAINER_OF + "]->(postA2),\n"
                   + " (forumA1)-[:" + Rels.CONTAINER_OF + "]->(postA3),\n"
                   + " (forumA1)-[:" + Rels.CONTAINER_OF + "]->(postA4),\n"
                   + " (forumA1)-[:" + Rels.CONTAINER_OF + "]->(postA5),\n"
                   + " (forumA1)-[:" + Rels.CONTAINER_OF + "]->(postA6),\n"
                   + " (forumA2)-[:" + Rels.CONTAINER_OF + "]->(postA7),\n"
                   + " (forumA2)-[:" + Rels.CONTAINER_OF + "]->(postA8),\n"
                   + " (forumA2)-[:" + Rels.CONTAINER_OF + "]->(postA9),\n"
                   + " (forumA4)-[:" + Rels.CONTAINER_OF + "]->(postA10),\n"
                   + " (forumA4)-[:" + Rels.CONTAINER_OF + "]->(postA11),\n"
                   + " (forumA4)-[:" + Rels.CONTAINER_OF + "]->(postA12),\n"
                   + " (forumB1)-[:" + Rels.CONTAINER_OF + "]->(postB1),\n"
                   + " (forumB1)-[:" + Rels.CONTAINER_OF + "]->(postB2),\n"
                   // Person Forum
                   + " (personA1)<-[:" + Rels.HAS_MODERATOR + "]-(forumA1),\n"
                   + " (personA2)<-[:" + Rels.HAS_MODERATOR + "]-(forumA2),\n"
                   + " (personA2)<-[:" + Rels.HAS_MODERATOR + "]-(forumA3),\n"
                   + " (personA2)<-[:" + Rels.HAS_MODERATOR + "]-(forumA4),\n"
                   + " (personB1)<-[:" + Rels.HAS_MODERATOR + "]-(forumB1),\n"
                   // City Person
                   + " (cityA1)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(personA1),\n"
                   + " (cityA1)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(personA2),\n"
                   + " (cityA2)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(personA3),\n"
                   + " (cityB1)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(personB1),\n"
                   // City Country
                   + " (countryA)<-[:" + Rels.IS_PART_OF + "]-(cityA1),\n"
                   + " (countryA)<-[:" + Rels.IS_PART_OF + "]-(cityA2),\n"
                   + " (countryB)<-[:" + Rels.IS_PART_OF + "]-(cityB1)\n"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query5GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Country
                   + " (country1:" + Place.Type.Country + "{" + Place.NAME + ":'Country1'}),\n"
                   + " (country2:" + Place.Type.Country + "{" + Place.NAME + ":'Country2'}),\n"
                   // City
                   + " (city1:" + Place.Type.City + "{" + Place.NAME + ":'City1'}),\n"
                   + " (city2:" + Place.Type.City + "{" + Place.NAME + ":'City2'}),\n"
                   + " (city3:" + Place.Type.City + "{" + Place.NAME + ":'City3'}),\n"
                   // Person
                   + " (person1:" + Nodes.Person + "{"
                   + Person.ID + ":1,"
                   + Person.FIRST_NAME + ":'First1',"
                   + Person.LAST_NAME + ":'Last1',"
                   + Person.CREATION_DATE + ":1"
                   + "}),\n"
                   + " (person2:" + Nodes.Person + "{"
                   + Person.ID + ":2,"
                   + Person.FIRST_NAME + ":'First2',"
                   + Person.LAST_NAME + ":'Last2',"
                   + Person.CREATION_DATE + ":2"
                   + "}),\n"
                   + " (person3:" + Nodes.Person + "{"
                   + Person.ID + ":3,"
                   + Person.FIRST_NAME + ":'First3',"
                   + Person.LAST_NAME + ":'Last3',"
                   + Person.CREATION_DATE + ":3"
                   + "}),\n"
                   + " (person4:" + Nodes.Person + "{"
                   + Person.ID + ":4,"
                   + Person.FIRST_NAME + ":'First4',"
                   + Person.LAST_NAME + ":'Last4',"
                   + Person.CREATION_DATE + ":4"
                   + "}),\n"
                   + " (person5:" + Nodes.Person + "{"
                   + Person.ID + ":5,"
                   + Person.FIRST_NAME + ":'First5',"
                   + Person.LAST_NAME + ":'Last5',"
                   + Person.CREATION_DATE + ":5"
                   + "}),\n"
                   + " (person6:" + Nodes.Person + "{"
                   + Person.ID + ":6,"
                   + Person.FIRST_NAME + ":'First6',"
                   + Person.LAST_NAME + ":'Last6',"
                   + Person.CREATION_DATE + ":6"
                   + "}),\n"
                   + " (person7:" + Nodes.Person + "{"
                   + Person.ID + ":7,"
                   + Person.FIRST_NAME + ":'First7',"
                   + Person.LAST_NAME + ":'Last7',"
                   + Person.CREATION_DATE + ":7"
                   + "}),\n"
                   // Forum
                   + " (forum1:" + Nodes.Forum + "{" + Forum.ID + ":1}),\n"
                   + " (forum2:" + Nodes.Forum + "{" + Forum.ID + ":2}),\n"
                   + " (forum3:" + Nodes.Forum + "{" + Forum.ID + ":3}),\n"
                   + " (forum4:" + Nodes.Forum + "{" + Forum.ID + ":4}),\n"
                   + " (forum5:" + Nodes.Forum + "{" + Forum.ID + ":5}),\n"
                   + " (forum6:" + Nodes.Forum + "{" + Forum.ID + ":6}),\n"
                   // Post
                   + " (post1:" + Nodes.Message + ":" + Nodes.Post + "),\n"
                   + " (post2:" + Nodes.Message + ":" + Nodes.Post + "),\n"
                   + " (post3:" + Nodes.Message + ":" + Nodes.Post + "),\n"
                   + " (post4:" + Nodes.Message + ":" + Nodes.Post + "),\n"
                   + " (post5:" + Nodes.Message + ":" + Nodes.Post + "),\n"
                   + " (post6:" + Nodes.Message + ":" + Nodes.Post + "),\n"
                   + " (post7:" + Nodes.Message + ":" + Nodes.Post + "),\n"
                   + " (post8:" + Nodes.Message + ":" + Nodes.Post + "),\n"
                   + " (post9:" + Nodes.Message + ":" + Nodes.Post + "),\n"
                   + " (post10:" + Nodes.Message + ":" + Nodes.Post + "),\n"
                   + " (post11:" + Nodes.Message + ":" + Nodes.Post + "),\n"

                   // Country City
                   + " (country1)<-[:" + Rels.IS_PART_OF + "]-(city1),\n"
                   + " (country1)<-[:" + Rels.IS_PART_OF + "]-(city2),\n"
                   + " (country4)<-[:" + Rels.IS_PART_OF + "]-(city3),\n"
                   // City Person
                   + " (city1)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person1),\n"
                   + " (city1)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person2),\n"
                   + " (city2)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person3),\n"
                   + " (city2)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person4),\n"
                   + " (city2)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person5),\n"
                   + " (city3)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person6),\n"
                   + " (city3)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person7),\n"
                   // Person Forum
                   + " (person1)<-[:" + Rels.HAS_MEMBER + "]-(forum1),\n"
                   + " (person2)<-[:" + Rels.HAS_MEMBER + "]-(forum1),\n"
                   + " (person1)<-[:" + Rels.HAS_MEMBER + "]-(forum2),\n"
                   + " (person2)<-[:" + Rels.HAS_MEMBER + "]-(forum2),\n"
                   + " (person3)<-[:" + Rels.HAS_MEMBER + "]-(forum2),\n"
                   + " (person4)<-[:" + Rels.HAS_MEMBER + "]-(forum2),\n"
                   + " (person2)<-[:" + Rels.HAS_MEMBER + "]-(forum3),\n"
                   + " (person4)<-[:" + Rels.HAS_MEMBER + "]-(forum3),\n"
                   + " (person5)<-[:" + Rels.HAS_MEMBER + "]-(forum3),\n"
                   + " (person4)<-[:" + Rels.HAS_MEMBER + "]-(forum4),\n"
                   + " (person5)<-[:" + Rels.HAS_MEMBER + "]-(forum4),\n"
                   + " (person6)<-[:" + Rels.HAS_MEMBER + "]-(forum4),\n"
                   + " (person5)<-[:" + Rels.HAS_MEMBER + "]-(forum5),\n"
                   + " (person6)<-[:" + Rels.HAS_MEMBER + "]-(forum6),\n"
                   + " (person7)<-[:" + Rels.HAS_MEMBER + "]-(forum6),\n"
                   // Forum Post
                   + " (forum1)-[:" + Rels.CONTAINER_OF + "]->(post1),\n"
                   + " (forum1)-[:" + Rels.CONTAINER_OF + "]->(post2),\n"
                   + " (forum1)-[:" + Rels.CONTAINER_OF + "]->(post3),\n"
                   + " (forum2)-[:" + Rels.CONTAINER_OF + "]->(post4),\n"
                   + " (forum2)-[:" + Rels.CONTAINER_OF + "]->(post5),\n"
                   + " (forum3)-[:" + Rels.CONTAINER_OF + "]->(post6),\n"
                   + " (forum4)-[:" + Rels.CONTAINER_OF + "]->(post7),\n"
                   + " (forum4)-[:" + Rels.CONTAINER_OF + "]->(post8),\n"
                   + " (forum4)-[:" + Rels.CONTAINER_OF + "]->(post9),\n"
                   + " (forum6)-[:" + Rels.CONTAINER_OF + "]->(post10),\n"
                   + " (forum6)-[:" + Rels.CONTAINER_OF + "]->(post11),\n"
                   // Post Person
                   + " (post1)-[:" + Rels.POST_HAS_CREATOR + "]->(person2),\n"
                   + " (post2)-[:" + Rels.POST_HAS_CREATOR + "]->(person2),\n"
                   + " (post3)-[:" + Rels.POST_HAS_CREATOR + "]->(person2),\n"
                   + " (post4)-[:" + Rels.POST_HAS_CREATOR + "]->(person3),\n"
                   + " (post5)-[:" + Rels.POST_HAS_CREATOR + "]->(person3),\n"
                   + " (post6)-[:" + Rels.POST_HAS_CREATOR + "]->(person4),\n"
                   + " (post7)-[:" + Rels.POST_HAS_CREATOR + "]->(person6),\n"
                   + " (post8)-[:" + Rels.POST_HAS_CREATOR + "]->(person6),\n"
                   + " (post9)-[:" + Rels.POST_HAS_CREATOR + "]->(person6),\n"
                   + " (post10)-[:" + Rels.POST_HAS_CREATOR + "]->(person6),\n"
                   + " (post11)-[:" + Rels.POST_HAS_CREATOR + "]->(person7)\n"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query6GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Person
                   + " (person1:" + Nodes.Person + " {" + Person.ID + ":1}),\n"
                   + " (person2:" + Nodes.Person + " {" + Person.ID + ":2}),\n"
                   + " (person3:" + Nodes.Person + " {" + Person.ID + ":3}),\n"
                   + " (person4:" + Nodes.Person + " {" + Person.ID + ":4}),\n"
                   + " (person5:" + Nodes.Person + " {" + Person.ID + ":5}),\n"

                   // Post
                   + " (post1:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.ID + ":1}),\n"
                   + " (post2:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.ID + ":2}),\n"
                   + " (post3:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.ID + ":3}),\n"

                   // Comment
                   + " (comment11:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":11}),\n"
                   + " (comment12:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":12}),\n"
                   + " (comment111:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":111}),\n"
                   + " (comment121:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":121}),\n"
                   + " (comment122:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":122}),\n"
                   + " (comment123:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":123}),\n"
                   + " (comment124:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":124}),\n"
                   + " (comment21:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":21}),\n"
                   + " (comment31:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":31}),\n"
                   + " (comment32:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":32}),\n"
                   + " (comment33:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":33}),\n"

                   // Tag
                   + " (tag1:" + Nodes.Tag + " {name:'tag1'}),\n"
                   + " (tag2:" + Nodes.Tag + " {name:'tag2'}),\n"

                   // Person HAS_CREATOR Post
                   + " (person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(post1),\n"
                   + " (person2)<-[:" + Rels.POST_HAS_CREATOR + "]-(post2),\n"
                   + " (person3)<-[:" + Rels.POST_HAS_CREATOR + "]-(post3),\n"

                   // Person HAS_CREATOR Comment
                   + " (person1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment111),\n"
                   + " (person1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment121),\n"
                   + " (person1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment124),\n"
                   + " (person2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment11),\n"
                   + " (person2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment122),\n"
                   + " (person3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment12),\n"
                   + " (person3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment123),\n"
                   + " (person4)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment21),\n"
                   + " (person4)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment31),\n"
                   + " (person5)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment32),\n"
                   + " (person5)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment33),\n"

                   // Comment REPLY Post
                   + " (post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment11),\n"
                   + " (post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment12),\n"
                   + " (post2)<-[:" + Rels.REPLY_OF_POST + "]-(comment21),\n"
                   + " (post3)<-[:" + Rels.REPLY_OF_POST + "]-(comment31),\n"
                   + " (post3)<-[:" + Rels.REPLY_OF_POST + "]-(comment32),\n"
                   + " (post3)<-[:" + Rels.REPLY_OF_POST + "]-(comment33),\n"

                   // Comment REPLY Comment
                   + " (comment11)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment111),\n"
                   + " (comment12)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment121),\n"
                   + " (comment12)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment122),\n"
                   + " (comment12)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment123),\n"
                   + " (comment12)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment124),\n"

                   // Person LIKE Post
                   + " (person2)-[:" + Rels.LIKES_POST + "]->(post1),\n"

                   // Person LIKE Comment
                   + " (person3)-[:" + Rels.LIKES_COMMENT + "]->(comment11),\n"
                   + " (person3)-[:" + Rels.LIKES_COMMENT + "]->(comment21),\n"
                   + " (person5)-[:" + Rels.LIKES_COMMENT + "]->(comment21),\n"

                   // Tag Post
                   + " (tag1)<-[:" + Rels.POST_HAS_TAG + "]-(post1),\n"
                   + " (tag1)<-[:" + Rels.POST_HAS_TAG + "]-(post2),\n"
                   + " (tag2)<-[:" + Rels.POST_HAS_TAG + "]-(post3),\n"

                   // Tag Comment
                   + " (tag1)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment11),\n"
                   + " (tag1)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment12),\n"
                   + " (tag1)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment124),\n"
                   + " (tag1)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment21),\n"
                   + " (tag1)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment33),\n"
                   + " (tag2)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment111),\n"
                   + " (tag2)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment121),\n"
                   + " (tag2)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment122),\n"
                   + " (tag2)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment123),\n"
                   + " (tag2)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment31),\n"
                   + " (tag2)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment32)\n"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query7GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Person
                   + " (person1:" + Nodes.Person + " {" + Person.ID + ":1}),\n"
                   + " (person2:" + Nodes.Person + " {" + Person.ID + ":2}),\n"
                   + " (person3:" + Nodes.Person + " {" + Person.ID + ":3}),\n"
                   + " (person4:" + Nodes.Person + " {" + Person.ID + ":4}),\n"
                   + " (person5:" + Nodes.Person + " {" + Person.ID + ":5}),\n"
                   + " (person6:" + Nodes.Person + " {" + Person.ID + ":6}),\n"
                   + " (person7:" + Nodes.Person + " {" + Person.ID + ":7}),\n"

                   // Post
                   + " (post0:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.ID + ":0}),\n"
                   + " (post1:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.ID + ":1}),\n"
                   + " (post2:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.ID + ":2}),\n"
                   + " (post3:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.ID + ":3}),\n"

                   // Comment
                   + " (comment4:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":4}),\n"
                   + " (comment5:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":5}),\n"
                   + " (comment6:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":6}),\n"
                   + " (comment7:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":7}),\n"
                   + " (comment8:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.ID + ":8}),\n"

                   // Tag
                   + " (tagGood:" + Nodes.Tag + " {" + Tag.NAME + ":'good'}),\n"
                   + " (tagBad:" + Nodes.Tag + " {" + Tag.NAME + ":'bad'}),\n"

                   // Person HAS_CREATOR Post
                   + " (person2)<-[:" + Rels.POST_HAS_CREATOR + "]-(post0),\n"
                   + " (person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(post1),\n"
                   + " (person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(post2),\n"
                   + " (person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(post3),\n"

                   // Person HAS_CREATOR Comment
                   + " (person1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment4),\n"
                   + " (person5)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment5),\n"
                   + " (person6)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment6),\n"
                   + " (person6)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment7),\n"
                   + " (person7)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment8),\n"

                   // Person LIKED Post
                   + " (person1)-[:" + Rels.LIKES_POST + "]->(post0),\n"
                   + " (person2)-[:" + Rels.LIKES_POST + "]->(post1),\n"
                   + " (person3)-[:" + Rels.LIKES_POST + "]->(post3),\n"
                   + " (person5)-[:" + Rels.LIKES_POST + "]->(post3),\n"

                   // Person LIKED Comment
                   + " (person3)-[:" + Rels.LIKES_COMMENT + "]->(comment4),\n"
                   + " (person6)-[:" + Rels.LIKES_COMMENT + "]->(comment4),\n"
                   + " (person3)-[:" + Rels.LIKES_COMMENT + "]->(comment5),\n"
                   + " (person4)-[:" + Rels.LIKES_COMMENT + "]->(comment5),\n"
                   + " (person5)-[:" + Rels.LIKES_COMMENT + "]->(comment6),\n"
                   + " (person4)-[:" + Rels.LIKES_COMMENT + "]->(comment7),\n"
                   + " (person5)-[:" + Rels.LIKES_COMMENT + "]->(comment7),\n"

                   // Post HAS_TAG Tag
                   + " (post0)-[:" + Rels.POST_HAS_TAG + "]->(tagGood),\n"
                   + " (post2)-[:" + Rels.POST_HAS_TAG + "]->(tagGood),\n"
                   + " (post3)-[:" + Rels.POST_HAS_TAG + "]->(tagGood),\n"
                   + " (post3)-[:" + Rels.POST_HAS_TAG + "]->(tagBad),\n"

                   // Comment HAS_TAG Tag
                   + " (comment4)-[:" + Rels.COMMENT_HAS_TAG + "]->(tagGood),\n"
                   + " (comment4)-[:" + Rels.COMMENT_HAS_TAG + "]->(tagBad),\n"
                   + " (comment6)-[:" + Rels.COMMENT_HAS_TAG + "]->(tagGood),\n"
                   + " (comment8)-[:" + Rels.COMMENT_HAS_TAG + "]->(tagGood)\n"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query8GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Post
                   + " (post1:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post1'}),\n"
                   + " (post2:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post2'}),\n"
                   + " (post3:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post3'}),\n"
                   + " (post4:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post4'}),\n"

                   // Comment
                   + " (comment5:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.CONTENT + ":'comment5'}),\n"
                   + " (comment6:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.CONTENT + ":'comment6'}),\n"
                   + " (comment7:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.CONTENT + ":'comment7'}),\n"
                   + " (comment8:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.CONTENT + ":'comment8'}),\n"
                   + " (comment9:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.CONTENT + ":'comment9'}),\n"
                   + " (comment10:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.CONTENT + ":'comment10'}),\n"
                   + " (comment11:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.CONTENT + ":'comment11'}),\n"

                   // Tag
                   + " (tag1:" + Nodes.Tag + "{" + Tag.NAME + ":'tag1'}),\n"
                   + " (tag2:" + Nodes.Tag + "{" + Tag.NAME + ":'tag2'}),\n"
                   + " (tag3:" + Nodes.Tag + "{" + Tag.NAME + ":'tag3'}),\n"
                   + " (tag4:" + Nodes.Tag + "{" + Tag.NAME + ":'tag4'}),\n"
                   + " (tag5:" + Nodes.Tag + "{" + Tag.NAME + ":'tag5'}),\n"

                   // Post REPLY Comment
                   + " (post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment8),\n"
                   + " (post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment9),\n"
                   + " (post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment10),\n"
                   + " (post3)<-[:" + Rels.REPLY_OF_POST + "]-(comment7),\n"
                   + " (post4)<-[:" + Rels.REPLY_OF_POST + "]-(comment5),\n"
                   + " (post4)<-[:" + Rels.REPLY_OF_POST + "]-(comment6),\n"

                   // Comment REPLY Comment
                   + " (comment5)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment11),\n"

                   // Post HAS Tag
                   + " (post1)-[:" + Rels.POST_HAS_TAG + "]->(tag1),\n"
                   + " (post2)-[:" + Rels.POST_HAS_TAG + "]->(tag1),\n"
                   + " (post3)-[:" + Rels.POST_HAS_TAG + "]->(tag2),\n"
                   + " (post4)-[:" + Rels.POST_HAS_TAG + "]->(tag2),\n"

                   // Comment HAS Tag
                   + " (comment5)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag1),\n"
                   + " (comment5)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag3),\n"
                   + " (comment6)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag2),\n"
                   + " (comment8)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag1),\n"
                   + " (comment8)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag2),\n"
                   + " (comment8)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag3),\n"
                   + " (comment8)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag4),\n"
                   + " (comment8)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag5),\n"
                   + " (comment9)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag2),\n"
                   + " (comment9)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag3),\n"
                   + " (comment9)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag4),\n"
                   + " (comment10)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag3),\n"
                   + " (comment10)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag5),\n"
                   + " (comment11)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag2),\n"
                   + " (comment11)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag3)\n"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query9GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Person
                   + " (person1:" + Nodes.Person + " {" + Person.ID + ":1}),\n"
                   + " (person2:" + Nodes.Person + " {" + Person.ID + ":2}),\n"
                   + " (person3:" + Nodes.Person + " {" + Person.ID + ":3}),\n"
                   + " (person4:" + Nodes.Person + " {" + Person.ID + ":4}),\n"
                   + " (person5:" + Nodes.Person + " {" + Person.ID + ":5}),\n"
                   + " (person6:" + Nodes.Person + " {" + Person.ID + ":6}),\n"
                   + " (person7:" + Nodes.Person + " {" + Person.ID + ":7}),\n"

                   // Forum
                   + " (forum1:" + Nodes.Forum + " {" + Forum.ID + ":1}),\n"
                   + " (forum2:" + Nodes.Forum + " {" + Forum.ID + ":2}),\n"
                   + " (forum3:" + Nodes.Forum + " {" + Forum.ID + ":3}),\n"
                   + " (forum4:" + Nodes.Forum + " {" + Forum.ID + ":4}),\n"
                   + " (forum5:" + Nodes.Forum + " {" + Forum.ID + ":5}),\n"
                   + " (forum6:" + Nodes.Forum + " {" + Forum.ID + ":6}),\n"

                   // Post
                   + " (post11:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post11'}),\n"
                   + " (post12:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post12'}),\n"
                   + " (post13:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post13'}),\n"
                   + " (post14:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post14'}),\n"
                   + " (post15:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post15'}),\n"
                   + " (post21:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post21'}),\n"
                   + " (post22:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post22'}),\n"
                   + " (post23:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post23'}),\n"
                   + " (post24:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post24'}),\n"
                   + " (post31:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post31'}),\n"
                   + " (post32:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post32'}),\n"
                   + " (post33:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post33'}),\n"
                   + " (post41:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post41'}),\n"
                   + " (post42:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post42'}),\n"
                   + " (post43:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post43'}),\n"
                   + " (post51:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post51'}),\n"
                   + " (post61:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post61'}),\n"

                   // Tag
                   + " (tag11:" + Nodes.Tag + " {" + Tag.NAME + ":'tag11'}),\n"
                   + " (tag12:" + Nodes.Tag + " {" + Tag.NAME + ":'tag12'}),\n"
                   + " (tag13:" + Nodes.Tag + " {" + Tag.NAME + ":'tag13'}),\n"
                   + " (tag14:" + Nodes.Tag + " {" + Tag.NAME + ":'tag14'}),\n"
                   + " (tag21:" + Nodes.Tag + " {" + Tag.NAME + ":'tag21'}),\n"
                   + " (tag22:" + Nodes.Tag + " {" + Tag.NAME + ":'tag22'}),\n"
                   + " (tag23:" + Nodes.Tag + " {" + Tag.NAME + ":'tag23'}),\n"
                   + " (tag24:" + Nodes.Tag + " {" + Tag.NAME + ":'tag24'}),\n"
                   + " (tag31:" + Nodes.Tag + " {" + Tag.NAME + ":'tag31'}),\n"
                   + " (tag32:" + Nodes.Tag + " {" + Tag.NAME + ":'tag32'}),\n"

                   // Tag Class
                   + " (tagClass1:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass1'}),\n"
                   + " (tagClass2:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass2'}),\n"
                   + " (tagClass3:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass3'}),\n"

                   // Person MEMBER Forum
                   + " (person1)<-[:" + Rels.HAS_MEMBER + "]-(forum1),\n"
                   + " (person2)<-[:" + Rels.HAS_MEMBER + "]-(forum1),\n"
                   + " (person2)<-[:" + Rels.HAS_MEMBER + "]-(forum2),\n"
                   + " (person3)<-[:" + Rels.HAS_MEMBER + "]-(forum1),\n"
                   + " (person3)<-[:" + Rels.HAS_MEMBER + "]-(forum2),\n"
                   + " (person4)<-[:" + Rels.HAS_MEMBER + "]-(forum2),\n"
                   + " (person4)<-[:" + Rels.HAS_MEMBER + "]-(forum3),\n"
                   + " (person5)<-[:" + Rels.HAS_MEMBER + "]-(forum3),\n"
                   + " (person5)<-[:" + Rels.HAS_MEMBER + "]-(forum4),\n"
                   + " (person6)<-[:" + Rels.HAS_MEMBER + "]-(forum3),\n"
                   + " (person6)<-[:" + Rels.HAS_MEMBER + "]-(forum4),\n"
                   + " (person6)<-[:" + Rels.HAS_MEMBER + "]-(forum5),\n"
                   + " (person7)<-[:" + Rels.HAS_MEMBER + "]-(forum4),\n"
                   + " (person7)<-[:" + Rels.HAS_MEMBER + "]-(forum5),\n"
                   + " (person7)<-[:" + Rels.HAS_MEMBER + "]-(forum6),\n"

                   // Forum CONTAINS Post
                   + " (forum1)-[:" + Rels.CONTAINER_OF + "]->(post11),\n"
                   + " (forum1)-[:" + Rels.CONTAINER_OF + "]->(post12),\n"
                   + " (forum1)-[:" + Rels.CONTAINER_OF + "]->(post13),\n"
                   + " (forum1)-[:" + Rels.CONTAINER_OF + "]->(post14),\n"
                   + " (forum1)-[:" + Rels.CONTAINER_OF + "]->(post15),\n"
                   + " (forum2)-[:" + Rels.CONTAINER_OF + "]->(post21),\n"
                   + " (forum2)-[:" + Rels.CONTAINER_OF + "]->(post22),\n"
                   + " (forum2)-[:" + Rels.CONTAINER_OF + "]->(post23),\n"
                   + " (forum2)-[:" + Rels.CONTAINER_OF + "]->(post24),\n"
                   + " (forum3)-[:" + Rels.CONTAINER_OF + "]->(post31),\n"
                   + " (forum3)-[:" + Rels.CONTAINER_OF + "]->(post32),\n"
                   + " (forum3)-[:" + Rels.CONTAINER_OF + "]->(post33),\n"
                   + " (forum4)-[:" + Rels.CONTAINER_OF + "]->(post41),\n"
                   + " (forum4)-[:" + Rels.CONTAINER_OF + "]->(post42),\n"
                   + " (forum4)-[:" + Rels.CONTAINER_OF + "]->(post43),\n"
                   + " (forum5)-[:" + Rels.CONTAINER_OF + "]->(post51),\n"
                   + " (forum6)-[:" + Rels.CONTAINER_OF + "]->(post61),\n"

                   // Post HAS Tag
                   + " (post11)-[:" + Rels.POST_HAS_TAG + "]->(tag11),\n"
                   + " (post12)-[:" + Rels.POST_HAS_TAG + "]->(tag11),\n"
                   + " (post12)-[:" + Rels.POST_HAS_TAG + "]->(tag12),\n"
                   + " (post12)-[:" + Rels.POST_HAS_TAG + "]->(tag21),\n"
                   + " (post13)-[:" + Rels.POST_HAS_TAG + "]->(tag13),\n"
                   + " (post13)-[:" + Rels.POST_HAS_TAG + "]->(tag22),\n"
                   + " (post14)-[:" + Rels.POST_HAS_TAG + "]->(tag12),\n"
                   + " (post14)-[:" + Rels.POST_HAS_TAG + "]->(tag14),\n"
                   + " (post14)-[:" + Rels.POST_HAS_TAG + "]->(tag31),\n"
                   + " (post15)-[:" + Rels.POST_HAS_TAG + "]->(tag23),\n"
                   + " (post15)-[:" + Rels.POST_HAS_TAG + "]->(tag32),\n"
                   + " (post21)-[:" + Rels.POST_HAS_TAG + "]->(tag13),\n"
                   + " (post22)-[:" + Rels.POST_HAS_TAG + "]->(tag24),\n"
                   + " (post23)-[:" + Rels.POST_HAS_TAG + "]->(tag24),\n"
                   + " (post24)-[:" + Rels.POST_HAS_TAG + "]->(tag24),\n"
                   + " (post24)-[:" + Rels.POST_HAS_TAG + "]->(tag32),\n"
                   + " (post31)-[:" + Rels.POST_HAS_TAG + "]->(tag12),\n"
                   + " (post31)-[:" + Rels.POST_HAS_TAG + "]->(tag22),\n"
                   + " (post32)-[:" + Rels.POST_HAS_TAG + "]->(tag24),\n"
                   + " (post33)-[:" + Rels.POST_HAS_TAG + "]->(tag24),\n"
                   + " (post41)-[:" + Rels.POST_HAS_TAG + "]->(tag12),\n"
                   + " (post42)-[:" + Rels.POST_HAS_TAG + "]->(tag32),\n"
                   + " (post43)-[:" + Rels.POST_HAS_TAG + "]->(tag32),\n"
                   + " (post51)-[:" + Rels.POST_HAS_TAG + "]->(tag32),\n"
                   + " (post61)-[:" + Rels.POST_HAS_TAG + "]->(tag32),\n"

                   // Tag TYPE TagClass
                   + " (tag11)-[:" + Rels.HAS_TYPE + "]->(tagClass1),\n"
                   + " (tag12)-[:" + Rels.HAS_TYPE + "]->(tagClass1),\n"
                   + " (tag13)-[:" + Rels.HAS_TYPE + "]->(tagClass1),\n"
                   + " (tag14)-[:" + Rels.HAS_TYPE + "]->(tagClass1),\n"
                   + " (tag21)-[:" + Rels.HAS_TYPE + "]->(tagClass2),\n"
                   + " (tag22)-[:" + Rels.HAS_TYPE + "]->(tagClass2),\n"
                   + " (tag23)-[:" + Rels.HAS_TYPE + "]->(tagClass2),\n"
                   + " (tag24)-[:" + Rels.HAS_TYPE + "]->(tagClass2),\n"
                   + " (tag31)-[:" + Rels.HAS_TYPE + "]->(tagClass3),\n"
                   + " (tag32)-[:" + Rels.HAS_TYPE + "]->(tagClass3)\n"

                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query10GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Person
                   + " (person1:" + Nodes.Person + " {" + Person.ID + ":1}),\n"
                   + " (person2:" + Nodes.Person + " {" + Person.ID + ":2}),\n"
                   + " (person3:" + Nodes.Person + " {" + Person.ID + ":3}),\n"
                   + " (person4:" + Nodes.Person + " {" + Person.ID + ":4}),\n"
                   + " (person5:" + Nodes.Person + " {" + Person.ID + ":5}),\n"

                   // Tag
                   + " (tag1:" + Nodes.Tag + " {" + Tag.NAME + ":'tag1'}),\n"
                   + " (tag2:" + Nodes.Tag + " {" + Tag.NAME + ":'tag2'}),\n"

                   // Post
                   + " (post1:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post1'}),\n"
                   + " (post2:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post2'}),\n"
                   + " (post3:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post3'}),\n"
                   + " (post4:" + Nodes.Message + ":" + Nodes.Post + " {" + Message.CONTENT + ":'post4'}),\n"

                   // Comment
                   + " (comment5:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.CONTENT + ":'comment5'}),\n"
                   + " (comment6:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.CONTENT + ":'comment6'}),\n"
                   + " (comment7:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.CONTENT + ":'comment7'}),\n"
                   + " (comment8:" + Nodes.Message + ":" + Nodes.Comment + " {" + Message.CONTENT + ":'comment8'}),\n"

                   // Person KNOWS Person
                   + " (person1)<-[:" + Rels.KNOWS + "]-(person2),\n"
                   + " (person1)<-[:" + Rels.KNOWS + "]-(person5),\n"
                   + " (person2)<-[:" + Rels.KNOWS + "]-(person3),\n"
                   + " (person3)<-[:" + Rels.KNOWS + "]-(person4),\n"
                   + " (person4)<-[:" + Rels.KNOWS + "]-(person5),\n"

                   // Post HAS Tag
                   + " (post1)-[:" + Rels.POST_HAS_TAG + "]->(tag1),\n"
                   + " (post2)-[:" + Rels.POST_HAS_TAG + "]->(tag1),\n"
                   + " (post3)-[:" + Rels.POST_HAS_TAG + "]->(tag1),\n"
                   + " (post4)-[:" + Rels.POST_HAS_TAG + "]->(tag1),\n"

                   // Comment HAS Tag
                   + " (comment5)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag1),\n"
                   + " (comment6)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag1),\n"
                   + " (comment7)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag1),\n"
                   + " (comment7)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag2),\n"
                   + " (comment8)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag2),\n"

                   // Person CREATED Post
                   + " (person2)<-[:" + Rels.POST_HAS_CREATOR + "]-(post3),\n"
                   + " (person2)<-[:" + Rels.POST_HAS_CREATOR + "]-(post4),\n"
                   + " (person3)<-[:" + Rels.POST_HAS_CREATOR + "]-(post2),\n"
                   + " (person5)<-[:" + Rels.POST_HAS_CREATOR + "]-(post1),\n"

                   // Person CREATED Comment
                   + " (person1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment5),\n"
                   + " (person1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment6),\n"
                   + " (person3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment7),\n"
                   + " (person3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment8),\n"

                   // Person INTERESTED Tag
                   + " (person1)-[:" + Rels.HAS_INTEREST + "]->(tag1),\n"
                   + " (person2)-[:" + Rels.HAS_INTEREST + "]->(tag1),\n"
                   + " (person3)-[:" + Rels.HAS_INTEREST + "]->(tag1),\n"
                   + " (person3)-[:" + Rels.HAS_INTEREST + "]->(tag2),\n"
                   + " (person4)-[:" + Rels.HAS_INTEREST + "]->(tag2)\n"

                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query11GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Country
                   + " (country1:" + Place.Type.Country + " {" + Place.NAME + ":'country1'}),\n"
                   + " (country2:" + Place.Type.Country + " {" + Place.NAME + ":'country2'}),\n"

                   // City
                   + " (city11:" + Place.Type.City + " {" + Place.NAME + ":'city11'}),\n"
                   + " (city12:" + Place.Type.City + " {" + Place.NAME + ":'city12'}),\n"
                   + " (city21:" + Place.Type.City + " {" + Place.NAME + ":'city21'}),\n"
                   + " (city22:" + Place.Type.City + " {" + Place.NAME + ":'city22'}),\n"

                   // Person
                   + " (person11:" + Nodes.Person + " {" + Person.ID + ":11}),\n"
                   + " (person12:" + Nodes.Person + " {" + Person.ID + ":12}),\n"
                   + " (person13:" + Nodes.Person + " {" + Person.ID + ":13}),\n"
                   + " (person14:" + Nodes.Person + " {" + Person.ID + ":14}),\n"
                   + " (person21:" + Nodes.Person + " {" + Person.ID + ":21}),\n"
                   + " (person22:" + Nodes.Person + " {" + Person.ID + ":22}),\n"
                   + " (person23:" + Nodes.Person + " {" + Person.ID + ":23}),\n"
                   + " (person24:" + Nodes.Person + " {" + Person.ID + ":24}),\n"

                   // Post
                   + " (post11:" + Nodes.Post + "{" + Message.CONTENT + ":'a b c'}),\n"
                   + " (post12:" + Nodes.Post + "{" + Message.CONTENT + ":'a'}),\n"
                   + " (post13:" + Nodes.Post + "{" + Message.CONTENT + ":'a b c'}),\n"
                   + " (post21:" + Nodes.Post + "{" + Message.CONTENT + ":'a'}),\n"

                   // Comment
                   + " (comment14:" + Nodes.Comment + "{" + Message.CONTENT + ":'a'}),\n"
                   + " (comment15:" + Nodes.Comment + "{" + Message.CONTENT + ":'a c d'}),\n"
                   + " (comment16:" + Nodes.Comment + "{" + Message.CONTENT + ":'a c'}),\n"
                   + " (comment17:" + Nodes.Comment + "{" + Message.CONTENT + ":'c'}),\n"
                   + " (comment22:" + Nodes.Comment + "{" + Message.CONTENT + ":'a'}),\n"
                   + " (comment23:" + Nodes.Comment + "{" + Message.CONTENT + ":'a'}),\n"

                   // Tag
                   + " (tag1:" + Nodes.Tag + "{" + Tag.NAME + ":'tag1'}),\n"
                   + " (tag2:" + Nodes.Tag + "{" + Tag.NAME + ":'tag2'}),\n"
                   + " (tag3:" + Nodes.Tag + "{" + Tag.NAME + ":'tag3'}),\n"
                   + " (tag4:" + Nodes.Tag + "{" + Tag.NAME + ":'tag4'}),\n"
                   + " (tag5:" + Nodes.Tag + "{" + Tag.NAME + ":'tag5'}),\n"

                   // Country IS_PART_OF City
                   + " (country1)<-[:" + Rels.IS_PART_OF + "]-(city11),\n"
                   + " (country1)<-[:" + Rels.IS_PART_OF + "]-(city12),\n"
                   + " (country2)<-[:" + Rels.IS_PART_OF + "]-(city21),\n"
                   + " (country2)<-[:" + Rels.IS_PART_OF + "]-(city22),\n"

                   // City IS_LOCATED_IN Person
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person11),\n"
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person12),\n"
                   + " (city12)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person13),\n"
                   + " (city12)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person14),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person21),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person22),\n"
                   + " (city22)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person23),\n"
                   + " (city22)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person24),\n"

                   // Person HAS_CREATOR Post
                   + " (person11)<-[:" + Rels.POST_HAS_CREATOR + "]-(post11),\n"
                   + " (person12)<-[:" + Rels.POST_HAS_CREATOR + "]-(post12),\n"
                   + " (person13)<-[:" + Rels.POST_HAS_CREATOR + "]-(post13),\n"
                   + " (person21)<-[:" + Rels.POST_HAS_CREATOR + "]-(post21),\n"

                   // Person HAS_CREATOR Comment
                   + " (person14)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment14),\n"
                   + " (person14)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment15),\n"
                   + " (person14)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment16),\n"
                   + " (person14)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment17),\n"
                   + " (person22)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment22),\n"
                   + " (person23)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment23),\n"

                   // Post REPLY Comment
                   + " (post11)<-[:" + Rels.REPLY_OF_POST + "]-(comment14),\n"
                   + " (post12)<-[:" + Rels.REPLY_OF_POST + "]-(comment15),\n"
                   + " (post13)<-[:" + Rels.REPLY_OF_POST + "]-(comment17),\n"

                   // Post REPLY Comment
                   + " (comment22)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment16),\n"

                   // Post HAS_TAG Tag
                   + " (post11)-[:" + Rels.POST_HAS_TAG + "]->(tag1),\n"
                   + " (post12)-[:" + Rels.POST_HAS_TAG + "]->(tag2),\n"
                   + " (post13)-[:" + Rels.POST_HAS_TAG + "]->(tag3),\n"

                   // Comment HAS_TAG Tag
                   + " (comment14)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag1),\n"
                   + " (comment15)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag1),\n"
                   + " (comment16)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag1),\n"
                   + " (comment16)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag3),\n"
                   + " (comment17)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag4),\n"
                   + " (comment22)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag4),\n"
                   + " (comment22)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag5),\n"

                   // Person LIKE Comment
                   + " (person21)-[:" + Rels.LIKES_COMMENT + "]->(comment16),\n"
                   + " (person22)-[:" + Rels.LIKES_COMMENT + "]->(comment16),\n"
                   + " (person23)-[:" + Rels.LIKES_COMMENT + "]->(comment16),\n"
                   + " (person23)-[:" + Rels.LIKES_COMMENT + "]->(comment17),\n"
                   + " (person24)-[:" + Rels.LIKES_COMMENT + "]->(comment17)\n"

                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query12GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Person
                   + " (person1:" + Nodes.Person + " {"
                   + Person.ID + ":1,"
                   + Person.FIRST_NAME + ":'f1',"
                   + Person.LAST_NAME + ":'l1'"
                   + "}),\n"
                   + " (person2:" + Nodes.Person + " {"
                   + Person.ID + ":2,"
                   + Person.FIRST_NAME + ":'f2',"
                   + Person.LAST_NAME + ":'l2'"
                   + "}),\n"
                   + " (person3:" + Nodes.Person + " {"
                   + Person.ID + ":3,"
                   + Person.FIRST_NAME + ":'f3',"
                   + Person.LAST_NAME + ":'l3'"
                   + "}),\n"
                   + " (person4:" + Nodes.Person + " {"
                   + Person.ID + ":4,"
                   + Person.FIRST_NAME + ":'f4',"
                   + Person.LAST_NAME + ":'l4'"
                   + "}),\n"

                   // Post
                   + " (post1:" + Nodes.Message + ":" + Nodes.Post + " $post1),\n"
                   + " (post2:" + Nodes.Message + ":" + Nodes.Post + " $post2),\n"
                   + " (post3:" + Nodes.Message + ":" + Nodes.Post + " $post3),\n"
                   + " (post4:" + Nodes.Message + ":" + Nodes.Post + " $post4),\n"

                   // Comment
                   + " (comment5:" + Nodes.Message + ":" + Nodes.Comment + " $comment5),\n"
                   + " (comment6:" + Nodes.Message + ":" + Nodes.Comment + " $comment6),\n"
                   + " (comment7:" + Nodes.Message + ":" + Nodes.Comment + " $comment7),\n"
                   + " (comment8:" + Nodes.Message + ":" + Nodes.Comment + " $comment8),\n"

                   // Person HAS_CREATOR Post
                   + " (person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(post1),\n"
                   + " (person2)<-[:" + Rels.POST_HAS_CREATOR + "]-(post2),\n"
                   + " (person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(post3),\n"
                   + " (person2)<-[:" + Rels.POST_HAS_CREATOR + "]-(post4),\n"

                   // Person HAS_CREATOR Comment
                   + " (person3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment5),\n"
                   + " (person3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment6),\n"
                   + " (person3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment7),\n"
                   + " (person2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment8),\n"

                   // Person LIKE Post
                   + " (person1)-[:" + Rels.LIKES_POST + "]->(post1),\n"
                   + " (person1)-[:" + Rels.LIKES_POST + "]->(post2),\n"
                   + " (person1)-[:" + Rels.LIKES_POST + "]->(post3),\n"
                   + " (person1)-[:" + Rels.LIKES_POST + "]->(post4),\n"
                   + " (person2)-[:" + Rels.LIKES_POST + "]->(post2),\n"
                   + " (person2)-[:" + Rels.LIKES_POST + "]->(post4),\n"
                   + " (person3)-[:" + Rels.LIKES_POST + "]->(post2),\n"
                   + " (person3)-[:" + Rels.LIKES_POST + "]->(post3),\n"
                   + " (person3)-[:" + Rels.LIKES_POST + "]->(post4),\n"
                   + " (person4)-[:" + Rels.LIKES_POST + "]->(post3),\n"
                   + " (person4)-[:" + Rels.LIKES_POST + "]->(post4),\n"

                   // Person LIKE Comment
                   + " (person1)-[:" + Rels.LIKES_COMMENT + "]->(comment7),\n"
                   + " (person1)-[:" + Rels.LIKES_COMMENT + "]->(comment8),\n"
                   + " (person2)-[:" + Rels.LIKES_COMMENT + "]->(comment6),\n"
                   + " (person2)-[:" + Rels.LIKES_COMMENT + "]->(comment7),\n"
                   + " (person3)-[:" + Rels.LIKES_COMMENT + "]->(comment8),\n"
                   + " (person4)-[:" + Rels.LIKES_COMMENT + "]->(comment6),\n"
                   + " (person4)-[:" + Rels.LIKES_COMMENT + "]->(comment7),\n"
                   + " (person4)-[:" + Rels.LIKES_COMMENT + "]->(comment8)\n"

                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    "post1", MapUtil.map( Message.ID, 1L, Message.CREATION_DATE, date( 2012, 12, 1 ) ),
                    "post2", MapUtil.map( Message.ID, 2L, Message.CREATION_DATE, date( 2012, 11, 30 ) ),
                    "post3", MapUtil.map( Message.ID, 3L, Message.CREATION_DATE, date( 2012, 12, 2 ) ),
                    "post4", MapUtil.map( Message.ID, 4L, Message.CREATION_DATE, date( 2012, 12, 2 ) ),
                    "comment5", MapUtil.map( Message.ID, 5L, Message.CREATION_DATE, date( 2012, 12, 1 ) ),
                    "comment6", MapUtil.map( Message.ID, 6L, Message.CREATION_DATE, date( 2020, 2, 1 ) ),
                    "comment7", MapUtil.map( Message.ID, 7L, Message.CREATION_DATE, date( 2012, 12, 30 ) ),
                    "comment8", MapUtil.map( Message.ID, 8L, Message.CREATION_DATE, date( 2013, 11, 30 ) )
            );
        }
    }

    public static class Query13GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Country
                   + " (country1:" + Place.Type.Country + " {" + Place.NAME + ":'country1'}),\n"
                   + " (country2:" + Place.Type.Country + " {" + Place.NAME + ":'country2'}),\n"

                   // Post
                   + " (post1:" + Nodes.Message + ":" + Nodes.Post + " $post1),\n"
                   + " (post2:" + Nodes.Message + ":" + Nodes.Post + " $post2),\n"
                   + " (post3:" + Nodes.Message + ":" + Nodes.Post + " $post3),\n"
                   + " (post4:" + Nodes.Message + ":" + Nodes.Post + " $post4),\n"
                   + " (post5:" + Nodes.Message + ":" + Nodes.Post + " $post5),\n"

                   // Comment
                   + " (comment6:" + Nodes.Message + ":" + Nodes.Comment + " $comment6),\n"
                   + " (comment7:" + Nodes.Message + ":" + Nodes.Comment + " $comment7),\n"
                   + " (comment8:" + Nodes.Message + ":" + Nodes.Comment + " $comment8),\n"
                   + " (comment9:" + Nodes.Message + ":" + Nodes.Comment + " $comment9),\n"
                   + " (comment10:" + Nodes.Message + ":" + Nodes.Comment + " $comment10),\n"

                   // Tag
                   + " (tag1:" + Nodes.Tag + " {" + Tag.NAME + ":'tag1'}),\n"
                   + " (tag2:" + Nodes.Tag + " {" + Tag.NAME + ":'tag2'}),\n"
                   + " (tag3:" + Nodes.Tag + " {" + Tag.NAME + ":'tag3'}),\n"
                   + " (tag4:" + Nodes.Tag + " {" + Tag.NAME + ":'tag4'}),\n"
                   + " (tag5:" + Nodes.Tag + " {" + Tag.NAME + ":'tag5'}),\n"
                   + " (tag6:" + Nodes.Tag + " {" + Tag.NAME + ":'tag6'}),\n"
                   + " (tag7:" + Nodes.Tag + " {" + Tag.NAME + ":'tag7'}),\n"
                   + " (tag8:" + Nodes.Tag + " {" + Tag.NAME + ":'tag8'}),\n"
                   + " (tag9:" + Nodes.Tag + " {" + Tag.NAME + ":'tag9'}),\n"
                   + " (tag10:" + Nodes.Tag + " {" + Tag.NAME + ":'tag10'}),\n"

                   // Country IS_LOCATED_IN Post
                   + " (country1)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post1),\n"
                   + " (country1)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post2),\n"
                   + " (country1)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post3),\n"
                   + " (country2)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post4),\n"
                   + " (country1)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post5),\n"

                   // Country IS_LOCATED_IN Comment
                   + " (country1)<-[:" + Rels.COMMENT_IS_LOCATED_IN + "]-(comment6),\n"
                   + " (country1)<-[:" + Rels.COMMENT_IS_LOCATED_IN + "]-(comment7),\n"
                   + " (country1)<-[:" + Rels.COMMENT_IS_LOCATED_IN + "]-(comment8),\n"
                   + " (country2)<-[:" + Rels.COMMENT_IS_LOCATED_IN + "]-(comment9),\n"
                   + " (country1)<-[:" + Rels.COMMENT_IS_LOCATED_IN + "]-(comment10),\n"

                   // Post HAS_TAG Post
                   + " (post1)-[:" + Rels.POST_HAS_TAG + "]->(tag1),\n"
                   + " (post2)-[:" + Rels.POST_HAS_TAG + "]->(tag1),\n"
                   + " (post2)-[:" + Rels.POST_HAS_TAG + "]->(tag2),\n"
                   + " (post3)-[:" + Rels.POST_HAS_TAG + "]->(tag5),\n"
                   + " (post4)-[:" + Rels.POST_HAS_TAG + "]->(tag1),\n"
                   + " (post5)-[:" + Rels.POST_HAS_TAG + "]->(tag3),\n"

                   // Post HAS_TAG Comment
                   + " (comment6)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag1),\n"
                   + " (comment6)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag2),\n"
                   + " (comment6)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag3),\n"
                   + " (comment6)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag4),\n"
                   + " (comment6)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag5),\n"
                   + " (comment6)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag6),\n"
                   + " (comment7)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag7),\n"
                   + " (comment7)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag8),\n"
                   + " (comment7)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag9),\n"
                   + " (comment8)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag3),\n"
                   + " (comment9)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag6),\n"
                   + " (comment10)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag10)\n"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    "post1", MapUtil.map( Message.CREATION_DATE, date( 2000, 2 ) ),
                    "post2", MapUtil.map( Message.CREATION_DATE, date( 2000, 2 ) ),
                    "post3", MapUtil.map( Message.CREATION_DATE, date( 2001, 3 ) ),
                    "post4", MapUtil.map( Message.CREATION_DATE, date( 2000, 3 ) ),
                    "post5", MapUtil.map( Message.CREATION_DATE, date( 2001, 2 ) ),
                    "comment6", MapUtil.map( Message.CREATION_DATE, date( 2000, 2 ) ),
                    "comment7", MapUtil.map( Message.CREATION_DATE, date( 2000, 3 ) ),
                    "comment8", MapUtil.map( Message.CREATION_DATE, date( 2001, 2 ) ),
                    "comment9", MapUtil.map( Message.CREATION_DATE, date( 2000, 2 ) ),
                    "comment10", MapUtil.map( Message.CREATION_DATE, date( 2001, 3 ) )
            );
        }
    }

    public static class Query14GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Person
                   + " (person1:" + Nodes.Person + " {"
                   + Person.ID + ":1,"
                   + Person.FIRST_NAME + ":'f1',"
                   + Person.LAST_NAME + ":'l1'"
                   + "}),\n"
                   + " (person2:" + Nodes.Person + " {"
                   + Person.ID + ":2,"
                   + Person.FIRST_NAME + ":'f2',"
                   + Person.LAST_NAME + ":'l2'"
                   + "}),\n"
                   + " (person3:" + Nodes.Person + " {"
                   + Person.ID + ":3,"
                   + Person.FIRST_NAME + ":'f3',"
                   + Person.LAST_NAME + ":'l3'"
                   + "}),\n"
                   + " (person4:" + Nodes.Person + " {"
                   + Person.ID + ":4,"
                   + Person.FIRST_NAME + ":'f4',"
                   + Person.LAST_NAME + ":'l4'"
                   + "}),\n"
                   + " (person5:" + Nodes.Person + " {"
                   + Person.ID + ":5,"
                   + Person.FIRST_NAME + ":'f5',"
                   + Person.LAST_NAME + ":'l5'"
                   + "}),\n"

                   // Post
                   + " (post0:" + Nodes.Message + ":" + Nodes.Post + " $post0),\n"
                   + " (post1:" + Nodes.Message + ":" + Nodes.Post + " $post1),\n"
                   + " (post2:" + Nodes.Message + ":" + Nodes.Post + " $post2),\n"
                   + " (post3:" + Nodes.Message + ":" + Nodes.Post + " $post3),\n"
                   + " (post4:" + Nodes.Message + ":" + Nodes.Post + " $post4),\n"
                   + " (post5:" + Nodes.Message + ":" + Nodes.Post + " $post5),\n"

                   // Comment
                   + " (comment6:" + Nodes.Message + ":" + Nodes.Comment + " $comment6),\n"
                   + " (comment7:" + Nodes.Message + ":" + Nodes.Comment + " $comment7),\n"
                   + " (comment8:" + Nodes.Message + ":" + Nodes.Comment + " $comment8),\n"
                   + " (comment9:" + Nodes.Message + ":" + Nodes.Comment + " $comment9),\n"
                   + " (comment10:" + Nodes.Message + ":" + Nodes.Comment + " $comment10),\n"
                   + " (comment11:" + Nodes.Message + ":" + Nodes.Comment + " $comment11),\n"
                   + " (comment12:" + Nodes.Message + ":" + Nodes.Comment + " $comment12),\n"
                   + " (comment13:" + Nodes.Message + ":" + Nodes.Comment + " $comment13),\n"
                   + " (comment14:" + Nodes.Message + ":" + Nodes.Comment + " $comment14),\n"
                   + " (comment15:" + Nodes.Message + ":" + Nodes.Comment + " $comment15),\n"
                   + " (comment16:" + Nodes.Message + ":" + Nodes.Comment + " $comment16),\n"
                   + " (comment17:" + Nodes.Message + ":" + Nodes.Comment + " $comment17),\n"
                   + " (comment18:" + Nodes.Message + ":" + Nodes.Comment + " $comment18),\n"
                   + " (comment19:" + Nodes.Message + ":" + Nodes.Comment + " $comment19),\n"

                   // Person HAS_CREATOR Post
                   + " (person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(post1),\n"
                   + " (person2)<-[:" + Rels.POST_HAS_CREATOR + "]-(post2),\n"
                   + " (person3)<-[:" + Rels.POST_HAS_CREATOR + "]-(post3),\n"
                   + " (person3)<-[:" + Rels.POST_HAS_CREATOR + "]-(post5),\n"
                   + " (person4)<-[:" + Rels.POST_HAS_CREATOR + "]-(post4),\n"
                   + " (person5)<-[:" + Rels.POST_HAS_CREATOR + "]-(post0),\n"

                   // Post REPLY_OF Comment
                   + " (post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment6),\n"
                   + " (post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment7),\n"
                   + " (post2)<-[:" + Rels.REPLY_OF_POST + "]-(comment11),\n"
                   + " (post2)<-[:" + Rels.REPLY_OF_POST + "]-(comment12),\n"
                   + " (post3)<-[:" + Rels.REPLY_OF_POST + "]-(comment13),\n"
                   + " (post3)<-[:" + Rels.REPLY_OF_POST + "]-(comment14),\n"
                   + " (post3)<-[:" + Rels.REPLY_OF_POST + "]-(comment15),\n"

                   // Comment REPLY_OF Comment
                   + " (comment6)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment8),\n"
                   + " (comment6)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment9),\n"
                   + " (comment7)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment11),\n"
                   + " (comment8)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment10),\n"
                   + " (comment14)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment16),\n"
                   + " (comment14)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment17),\n"
                   + " (comment14)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment18)\n"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    "post0", MapUtil.map( Message.ID, 0L, Message.CREATION_DATE, date( 2012, 2, 3 ) ),
                    "post1", MapUtil.map( Message.ID, 1L, Message.CREATION_DATE, date( 2012, 2, 3 ) ),
                    "post2", MapUtil.map( Message.ID, 2L, Message.CREATION_DATE, date( 2012, 2, 2 ) ),
                    "post3", MapUtil.map( Message.ID, 3L, Message.CREATION_DATE, date( 2012, 2, 9 ) ),
                    "post4", MapUtil.map( Message.ID, 4L, Message.CREATION_DATE, date( 2012, 2, 7 ) ),
                    "post5", MapUtil.map( Message.ID, 5L, Message.CREATION_DATE, date( 2012, 2, 5 ) ),
                    "comment6", MapUtil.map( Message.ID, 6L, Message.CREATION_DATE, date( 2012, 2, 7 ) ),
                    "comment7", MapUtil.map( Message.ID, 7L, Message.CREATION_DATE, date( 2012, 2, 3 ) ),
                    "comment8", MapUtil.map( Message.ID, 8L, Message.CREATION_DATE, date( 2012, 2, 8 ) ),
                    "comment9", MapUtil.map( Message.ID, 9L, Message.CREATION_DATE, date( 2012, 2, 10 ) ),
                    "comment10", MapUtil.map( Message.ID, 10L, Message.CREATION_DATE, date( 2012, 2, 9 ) ),
                    "comment11", MapUtil.map( Message.ID, 11L, Message.CREATION_DATE, date( 2012, 2, 5 ) ),
                    "comment12", MapUtil.map( Message.ID, 12L, Message.CREATION_DATE, date( 2012, 2, 3 ) ),
                    "comment13", MapUtil.map( Message.ID, 13L, Message.CREATION_DATE, date( 2012, 2, 9 ) ),
                    "comment14", MapUtil.map( Message.ID, 14L, Message.CREATION_DATE, date( 2012, 2, 9 ) ),
                    "comment15", MapUtil.map( Message.ID, 15L, Message.CREATION_DATE, date( 2012, 2, 10 ) ),
                    "comment16", MapUtil.map( Message.ID, 16L, Message.CREATION_DATE, date( 2012, 2, 9 ) ),
                    "comment17", MapUtil.map( Message.ID, 17L, Message.CREATION_DATE, date( 2012, 2, 11 ) ),
                    "comment18", MapUtil.map( Message.ID, 18L, Message.CREATION_DATE, date( 2012, 2, 9 ) ),
                    "comment19", MapUtil.map( Message.ID, 19L, Message.CREATION_DATE, date( 2012, 2, 4 ) )
            );
        }
    }

    public static class Query15GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Country
                   + " (country1:" + Place.Type.Country + " {" + Place.NAME + ":'country1'}),\n"
                   + " (country2:" + Place.Type.Country + " {" + Place.NAME + ":'country2'}),\n"
                   + " (country3:" + Place.Type.Country + " {" + Place.NAME + ":'country3'}),\n"

                   // City
                   + " (city11:" + Place.Type.City + " {" + Place.NAME + ":'city11'}),\n"
                   + " (city12:" + Place.Type.City + " {" + Place.NAME + ":'city12'}),\n"
                   + " (city21:" + Place.Type.City + " {" + Place.NAME + ":'city21'}),\n"
                   + " (city22:" + Place.Type.City + " {" + Place.NAME + ":'city22'}),\n"
                   + " (city31:" + Place.Type.City + " {" + Place.NAME + ":'city31'}),\n"
                   + " (city32:" + Place.Type.City + " {" + Place.NAME + ":'city32'}),\n"

                   // Person
                   + " (person11:" + Nodes.Person + " {" + Person.ID + ":11}),\n"
                   + " (person12:" + Nodes.Person + " {" + Person.ID + ":12}),\n"
                   + " (person13:" + Nodes.Person + " {" + Person.ID + ":13}),\n"
                   + " (person14:" + Nodes.Person + " {" + Person.ID + ":14}),\n"
                   + " (person15:" + Nodes.Person + " {" + Person.ID + ":15}),\n"
                   + " (person16:" + Nodes.Person + " {" + Person.ID + ":16}),\n"

                   + " (person21:" + Nodes.Person + " {" + Person.ID + ":21}),\n"
                   + " (person22:" + Nodes.Person + " {" + Person.ID + ":22}),\n"
                   + " (person23:" + Nodes.Person + " {" + Person.ID + ":23}),\n"
                   + " (person24:" + Nodes.Person + " {" + Person.ID + ":24}),\n"
                   + " (person25:" + Nodes.Person + " {" + Person.ID + ":25}),\n"
                   + " (person26:" + Nodes.Person + " {" + Person.ID + ":26}),\n"

                   + " (person31:" + Nodes.Person + " {" + Person.ID + ":31}),\n"
                   + " (person32:" + Nodes.Person + " {" + Person.ID + ":32}),\n"
                   + " (person33:" + Nodes.Person + " {" + Person.ID + ":33}),\n"
                   + " (person34:" + Nodes.Person + " {" + Person.ID + ":34}),\n"
                   + " (person35:" + Nodes.Person + " {" + Person.ID + ":35}),\n"
                   + " (person36:" + Nodes.Person + " {" + Person.ID + ":36}),\n"

                   // Country IS_PART_OF City
                   + " (country1)<-[:" + Rels.IS_PART_OF + "]-(city11),\n"
                   + " (country1)<-[:" + Rels.IS_PART_OF + "]-(city12),\n"
                   + " (country2)<-[:" + Rels.IS_PART_OF + "]-(city21),\n"
                   + " (country2)<-[:" + Rels.IS_PART_OF + "]-(city22),\n"
                   + " (country3)<-[:" + Rels.IS_PART_OF + "]-(city31),\n"
                   + " (country3)<-[:" + Rels.IS_PART_OF + "]-(city42),\n"

                   // City IS_LOCATED_IN Person
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person11),\n"
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person12),\n"
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person13),\n"
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person14),\n"
                   + " (city12)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person15),\n"
                   + " (city12)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person16),\n"

                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person21),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person22),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person23),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person24),\n"
                   + " (city22)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person25),\n"
                   + " (city22)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person26),\n"

                   + " (city31)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person31),\n"
                   + " (city31)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person32),\n"
                   + " (city31)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person33),\n"
                   + " (city31)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person34),\n"
                   + " (city32)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person35),\n"
                   + " (city32)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person36),\n"

                   // Person KNOWS Person
                   + " (person11)<-[:" + Rels.KNOWS + "]-(person12),\n"
                   + " (person12)<-[:" + Rels.KNOWS + "]-(person13),\n"
                   + " (person13)<-[:" + Rels.KNOWS + "]-(person14),\n"
                   + " (person14)<-[:" + Rels.KNOWS + "]-(person15),\n"
                   + " (person15)<-[:" + Rels.KNOWS + "]-(person16),\n"
                   + " (person16)<-[:" + Rels.KNOWS + "]-(person11),\n"
                   + " (person11)<-[:" + Rels.KNOWS + "]-(person13),\n"
                   + " (person14)<-[:" + Rels.KNOWS + "]-(person16),\n"
                   + " (person13)<-[:" + Rels.KNOWS + "]-(person16),\n"

                   + " (person21)<-[:" + Rels.KNOWS + "]-(person22),\n"
                   + " (person22)<-[:" + Rels.KNOWS + "]-(person23),\n"
                   + " (person23)<-[:" + Rels.KNOWS + "]-(person24),\n"
                   + " (person24)<-[:" + Rels.KNOWS + "]-(person25),\n"
                   + " (person25)<-[:" + Rels.KNOWS + "]-(person26),\n"
                   + " (person26)<-[:" + Rels.KNOWS + "]-(person21),\n"

                   + " (person31)<-[:" + Rels.KNOWS + "]-(person32),\n"
                   + " (person32)<-[:" + Rels.KNOWS + "]-(person33),\n"
                   + " (person33)<-[:" + Rels.KNOWS + "]-(person34),\n"
                   + " (person34)<-[:" + Rels.KNOWS + "]-(person35),\n"
                   + " (person35)<-[:" + Rels.KNOWS + "]-(person36),\n"
                   + " (person36)<-[:" + Rels.KNOWS + "]-(person31),\n"

                   + " (person11)<-[:" + Rels.KNOWS + "]-(person31),\n"
                   + " (person11)<-[:" + Rels.KNOWS + "]-(person21),\n"
                   + " (person21)<-[:" + Rels.KNOWS + "]-(person31),\n"

                   + " (person16)<-[:" + Rels.KNOWS + "]-(person36),\n"
                   + " (person16)<-[:" + Rels.KNOWS + "]-(person26),\n"
                   + " (person26)<-[:" + Rels.KNOWS + "]-(person36)\n"

                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query16GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Country
                   + " (country1:" + Place.Type.Country + " {" + Place.NAME + ":'country1'}),\n"
                   + " (country2:" + Place.Type.Country + " {" + Place.NAME + ":'country2'}),\n"

                   // City
                   + " (city11:" + Place.Type.City + " {" + Place.NAME + ":'city11'}),\n"
                   + " (city12:" + Place.Type.City + " {" + Place.NAME + ":'city12'}),\n"

                   // TagClass
                   + " (tagClass1:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass1'}),\n"
                   + " (tagClass2:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass2'}),\n"

                   // Tag
                   + " (tag11:" + Nodes.Tag + " {" + Tag.NAME + ":'tag11'}),\n"
                   + " (tag12:" + Nodes.Tag + " {" + Tag.NAME + ":'tag12'}),\n"
                   + " (tag21:" + Nodes.Tag + " {" + Tag.NAME + ":'tag21'}),\n"

                   // Post
                   + " (post1:" + Nodes.Post + "),\n"
                   + " (post2:" + Nodes.Post + "),\n"

                   // Comment
                   + " (comment3:" + Nodes.Comment + "),\n"
                   + " (comment4:" + Nodes.Comment + "),\n"
                   + " (comment5:" + Nodes.Comment + "),\n"
                   + " (comment6:" + Nodes.Comment + "),\n"
                   + " (comment7:" + Nodes.Comment + "),\n"

                   // Person
                   + " (person11:" + Nodes.Person + " {" + Person.ID + ":11}),\n"
                   + " (person12:" + Nodes.Person + " {" + Person.ID + ":12}),\n"
                   + " (person13:" + Nodes.Person + " {" + Person.ID + ":13}),\n"
                   + " (person14:" + Nodes.Person + " {" + Person.ID + ":14}),\n"
                   + " (person20:" + Nodes.Person + " {" + Person.ID + ":20}),\n"
                   + " (person21:" + Nodes.Person + " {" + Person.ID + ":21}),\n"
                   + " (person22:" + Nodes.Person + " {" + Person.ID + ":22}),\n"
                   + " (person23:" + Nodes.Person + " {" + Person.ID + ":23}),\n"
                   + " (person24:" + Nodes.Person + " {" + Person.ID + ":24}),\n"

                   // Country IS_PART_OF City
                   + " (country1)<-[:" + Rels.IS_PART_OF + "]-(city11),\n"
                   + " (country2)<-[:" + Rels.IS_PART_OF + "]-(city21),\n"

                   // City IS_LOCATED_IN Person
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person11),\n"
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person12),\n"
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person13),\n"
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person14),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person20),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person21),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person22),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person23),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person24),\n"

                   // TagClass HAS_TYPE Tag
                   + " (tagClass1)<-[:" + Rels.HAS_TYPE + "]-(tag11),\n"
                   + " (tagClass1)<-[:" + Rels.HAS_TYPE + "]-(tag12),\n"
                   + " (tagClass2)<-[:" + Rels.HAS_TYPE + "]-(tag21),\n"

                   // Tag HAS_TAG Post
                   + " (tag11)<-[:" + Rels.POST_HAS_TAG + "]-(post1),\n"
                   + " (tag12)<-[:" + Rels.POST_HAS_TAG + "]-(post1),\n"
                   + " (tag12)<-[:" + Rels.POST_HAS_TAG + "]-(post2),\n"

                   // Tag HAS_TAG Comment
                   + " (tag11)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment5),\n"
                   + " (tag11)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment7),\n"
                   + " (tag12)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment3),\n"
                   + " (tag21)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment4),\n"
                   + " (tag21)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment6),\n"

                   // Post HAS_CREATOR Person
                   + " (post1)-[:" + Rels.POST_HAS_CREATOR + "]->(person11),\n"
                   + " (post2)-[:" + Rels.POST_HAS_CREATOR + "]->(person11),\n"

                   // Comment HAS_CREATOR Person
                   + " (comment3)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(person12),\n"
                   + " (comment4)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(person13),\n"
                   + " (comment5)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(person14),\n"
                   + " (comment6)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(person22),\n"
                   + " (comment7)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(person21),\n"

                   // Person KNOWS Person
                   + " (person20)<-[:" + Rels.KNOWS + "]-(person11),\n"
                   + " (person20)<-[:" + Rels.KNOWS + "]-(person21),\n"
                   + " (person21)<-[:" + Rels.KNOWS + "]-(person22),\n"
                   + " (person22)<-[:" + Rels.KNOWS + "]-(person23),\n"
                   + " (person23)<-[:" + Rels.KNOWS + "]-(person24),\n"
                   + " (person21)<-[:" + Rels.KNOWS + "]-(person14),\n"
                   + " (person13)<-[:" + Rels.KNOWS + "]-(person24)\n"

                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query17GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Country
                   + " (country1:" + Place.Type.Country + " {" + Place.NAME + ":'country1'}),\n"
                   + " (country2:" + Place.Type.Country + " {" + Place.NAME + ":'country2'}),\n"

                   // City
                   + " (city11:" + Place.Type.City + " {" + Place.NAME + ":'city11'}),\n"
                   + " (city12:" + Place.Type.City + " {" + Place.NAME + ":'city12'}),\n"
                   + " (city21:" + Place.Type.City + " {" + Place.NAME + ":'city21'}),\n"

                   // Person
                   + " (person11:" + Nodes.Person + " {" + Person.ID + ":11}),\n"
                   + " (person12:" + Nodes.Person + " {" + Person.ID + ":12}),\n"
                   + " (person13:" + Nodes.Person + " {" + Person.ID + ":13}),\n"
                   + " (person14:" + Nodes.Person + " {" + Person.ID + ":14}),\n"
                   + " (person15:" + Nodes.Person + " {" + Person.ID + ":15}),\n"
                   + " (person21:" + Nodes.Person + " {" + Person.ID + ":21}),\n"
                   + " (person22:" + Nodes.Person + " {" + Person.ID + ":22}),\n"
                   + " (person23:" + Nodes.Person + " {" + Person.ID + ":23}),\n"

                   // Country IS_PART_OF City
                   + " (country1)<-[:" + Rels.IS_PART_OF + "]-(city11),\n"
                   + " (country1)<-[:" + Rels.IS_PART_OF + "]-(city12),\n"
                   + " (country2)<-[:" + Rels.IS_PART_OF + "]-(city21),\n"

                   // Person IS_LOCATED_IN City
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person11),\n"
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person12),\n"
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person13),\n"
                   + " (city12)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person14),\n"
                   + " (city12)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person15),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person21),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person22),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person23),\n"

                   // Person KNOWS Person
                   + " (person11)<-[:" + Rels.KNOWS + "]-(person12),\n"
                   + " (person11)-[:" + Rels.KNOWS + "]->(person13),\n"
                   + " (person11)<-[:" + Rels.KNOWS + "]-(person15),\n"
                   + " (person11)-[:" + Rels.KNOWS + "]->(person21),\n"

                   + " (person12)<-[:" + Rels.KNOWS + "]-(person13),\n"
                   + " (person12)-[:" + Rels.KNOWS + "]->(person14),\n"

                   + " (person13)<-[:" + Rels.KNOWS + "]-(person14),\n"
                   + " (person13)-[:" + Rels.KNOWS + "]->(person15),\n"
                   + " (person13)<-[:" + Rels.KNOWS + "]-(person21),\n"
                   + " (person13)-[:" + Rels.KNOWS + "]->(person22),\n"
                   + " (person13)<-[:" + Rels.KNOWS + "]-(person23),\n"

                   + " (person14)<-[:" + Rels.KNOWS + "]-(person15),\n"

                   + " (person15)-[:" + Rels.KNOWS + "]->(person23),\n"

                   + " (person21)-[:" + Rels.KNOWS + "]->(person22),\n"
                   + " (person21)<-[:" + Rels.KNOWS + "]-(person23),\n"

                   + " (person22)-[:" + Rels.KNOWS + "]->(person23)\n"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query18GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Person
                   + " (person1:" + Nodes.Person + " {" + Person.ID + ":1}),\n"
                   + " (person2:" + Nodes.Person + " {" + Person.ID + ":2}),\n"
                   + " (person3:" + Nodes.Person + " {" + Person.ID + ":3}),\n"
                   + " (person4:" + Nodes.Person + " {" + Person.ID + ":4}),\n"

                   // Post
                   + " (post1:" + Nodes.Post + " $post1),\n"
                   + " (post2:" + Nodes.Post + " $post2),\n"
                   + " (post3:" + Nodes.Post + " $post3),\n"
                   + " (post4:" + Nodes.Post + " $post4),\n"
                   + " (post5:" + Nodes.Post + " $post5),\n"

                   // Comment
                   + " (comment6:" + Nodes.Post + " $comment6),\n"
                   + " (comment7:" + Nodes.Post + " $comment7),\n"
                   + " (comment8:" + Nodes.Post + " $comment8),\n"
                   + " (comment9:" + Nodes.Post + " $comment9),\n"
                   + " (comment10:" + Nodes.Post + " $comment10),\n"

                   // Person HAS_CREATOR Post
                   + " (person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(post2),\n"
                   + " (person2)<-[:" + Rels.POST_HAS_CREATOR + "]-(post1),\n"
                   + " (person2)<-[:" + Rels.POST_HAS_CREATOR + "]-(post3),\n"
                   + " (person3)<-[:" + Rels.POST_HAS_CREATOR + "]-(post4),\n"
                   + " (person3)<-[:" + Rels.POST_HAS_CREATOR + "]-(post5),\n"

                   // Person HAS_CREATOR Comment
                   + " (person1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment6),\n"
                   + " (person2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment8),\n"
                   + " (person3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment9),\n"
                   + " (person4)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment7),\n"
                   + " (person4)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment10)\n"

                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    "post1", MapUtil.map( Message.ID, 1L, Message.CREATION_DATE, date( 2000, 2, 1 ) ),
                    "post2", MapUtil.map( Message.ID, 2L, Message.CREATION_DATE, date( 2000, 2, 11 ) ),
                    "post3", MapUtil.map( Message.ID, 3L, Message.CREATION_DATE, date( 2001, 2, 1 ) ),
                    "post4", MapUtil.map( Message.ID, 4L, Message.CREATION_DATE, date( 2000, 3, 1 ) ),
                    "post5", MapUtil.map( Message.ID, 5L, Message.CREATION_DATE, date( 2000, 12, 31 ) ),
                    "comment6", MapUtil.map( Message.ID, 6L, Message.CREATION_DATE, date( 2000, 2, 9 ) ),
                    "comment7", MapUtil.map( Message.ID, 7L, Message.CREATION_DATE, date( 2000, 2, 10 ) ),
                    "comment8", MapUtil.map( Message.ID, 8L, Message.CREATION_DATE, date( 2000, 3, 10 ) ),
                    "comment9", MapUtil.map( Message.ID, 9L, Message.CREATION_DATE, date( 2000, 2, 11 ) ),
                    "comment10", MapUtil.map( Message.ID, 10L, Message.CREATION_DATE, date( 2000, 3, 1 ) )
            );
        }
    }

    public static class Query19GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Person
                   + " (p0:" + Nodes.Person + " {"
                   + Person.ID + ":" + 0 + ","
                   + Person.BIRTHDAY + ":" + date( 2000, 1, 1 )
                   + "}),\n"
                   + " (p1:" + Nodes.Person + " {"
                   + Person.ID + ":" + 1 + ","
                   + Person.BIRTHDAY + ":" + date( 2000, 1, 2 )
                   + "}),\n"
                   + " (p2:" + Nodes.Person + " {"
                   + Person.ID + ":" + 2 + ","
                   + Person.BIRTHDAY + ":" + date( 2000, 1, 2 )
                   + "}),\n"
                   + " (p3:" + Nodes.Person + " {"
                   + Person.ID + ":" + 3 + ","
                   + Person.BIRTHDAY + ":" + date( 2000, 1, 2 )
                   + "}),\n"
                   + " (p4:" + Nodes.Person + " {"
                   + Person.ID + ":" + 4 + ","
                   + Person.BIRTHDAY + ":" + date( 2000, 1, 2 )
                   + "}),\n"
                   + " (p5:" + Nodes.Person + " {"
                   + Person.ID + ":" + 5 + ","
                   + Person.BIRTHDAY + ":" + date( 2000, 1, 2 )
                   + "}),\n"
                   + " (p6:" + Nodes.Person + " {"
                   + Person.ID + ":" + 6 + ","
                   + Person.BIRTHDAY + ":" + date( 2000, 1, 2 )
                   + "}),\n"
                   + " (p7:" + Nodes.Person + " {"
                   + Person.ID + ":" + 7 + ","
                   + Person.BIRTHDAY + ":" + date( 2000, 1, 2 )
                   + "}),\n"
                   + " (p8:" + Nodes.Person + " {"
                   + Person.ID + ":" + 8 + ","
                   + Person.BIRTHDAY + ":" + date( 2000, 1, 2 )
                   + "}),\n"
                   + " (p9:" + Nodes.Person + " {"
                   + Person.ID + ":" + 9 + ","
                   + Person.BIRTHDAY + ":" + date( 2000, 1, 2 )
                   + "}),\n"
                   + " (p10:" + Nodes.Person + " {"
                   + Person.ID + ":" + 10 + ","
                   + Person.BIRTHDAY + ":" + date( 2000, 1, 2 )
                   + "}),\n"

                   // TagClass
                   + " (tagClass1:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass1'}),\n"
                   + " (tagClass2:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass2'}),\n"

                   // Tag
                   + " (tag1:" + Nodes.Tag + " {" + Tag.NAME + ":'tag1'}),\n"
                   + " (tag2:" + Nodes.Tag + " {" + Tag.NAME + ":'tag2'}),\n"

                   // Forum
                   + " (forum0:" + Nodes.Forum + " {" + Forum.TITLE + ":'forum0'}),\n"
                   + " (forum1:" + Nodes.Forum + " {" + Forum.TITLE + ":'forum1'}),\n"
                   + " (forum2:" + Nodes.Forum + " {" + Forum.TITLE + ":'forum2'}),\n"

                   // Post
                   + " (post0:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":" + 0 + "}),\n"
                   + " (post1:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":" + 1 + "}),\n"
                   + " (post2:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":" + 2 + "}),\n"
                   + " (post3:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":" + 3 + "}),\n"

                   // Comment
                   + " (comment1:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 11 + "}),\n"
                   + " (comment2:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 12 + "}),\n"
                   + " (comment3:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 13 + "}),\n"
                   + " (comment4:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 14 + "}),\n"
                   + " (comment5:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 15 + "}),\n"
                   + " (comment6:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 16 + "}),\n"
                   + " (comment7:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 17 + "}),\n"
                   + " (comment8:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 18 + "}),\n"
                   + " (comment9:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 19 + "}),\n"
                   + " (comment10:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 110 + "}),\n"
                   + " (comment11:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 111 + "}),\n"
                   + " (comment12:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 112 + "}),\n"
                   + " (comment13:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 113 + "}),\n"
                   + " (comment14:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 114 + "}),\n"
                   + " (comment15:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 115 + "}),\n"
                   + " (comment16:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 116 + "}),\n"
                   + " (comment17:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 117 + "}),\n"
                   + " (comment18:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":" + 118 + "}),\n"

                   // Tag HAS_TYPE TagClass
                   + " (tag1)-[:" + Rels.HAS_TYPE + "]->(tagClass1),\n"
                   + " (tag2)-[:" + Rels.HAS_TYPE + "]->(tagClass2),\n"

                   // Forum HAS_TAG Tag
                   + " (forum0)-[:" + Rels.FORUM_HAS_TAG + "]->(tag1),\n"
                   + " (forum1)-[:" + Rels.FORUM_HAS_TAG + "]->(tag2),\n"
                   + " (forum2)-[:" + Rels.FORUM_HAS_TAG + "]->(tag1),\n"
                   + " (forum2)-[:" + Rels.FORUM_HAS_TAG + "]->(tag2),\n"

                   // Forum HAS_MEMBER Person
                   + " (forum0)-[:" + Rels.HAS_MEMBER + "]->(p5),\n"
                   + " (forum0)-[:" + Rels.HAS_MEMBER + "]->(p7),\n"
                   + " (forum0)-[:" + Rels.HAS_MEMBER + "]->(p8),\n"
                   + " (forum0)-[:" + Rels.HAS_MEMBER + "]->(p10),\n"
                   + " (forum1)-[:" + Rels.HAS_MEMBER + "]->(p7),\n"
                   + " (forum1)-[:" + Rels.HAS_MEMBER + "]->(p10),\n"
                   + " (forum2)-[:" + Rels.HAS_MEMBER + "]->(p6),\n"
                   + " (forum2)-[:" + Rels.HAS_MEMBER + "]->(p8),\n"
                   + " (forum2)-[:" + Rels.HAS_MEMBER + "]->(p9),\n"

                   // Person KNOWS Person
                   + " (p1)-[:" + Rels.KNOWS + "]->(p5),\n"
                   + " (p2)<-[:" + Rels.KNOWS + "]-(p6),\n"
                   + " (p3)-[:" + Rels.KNOWS + "]->(p7),\n"

                   // Person HAS_CREATOR Post
                   + " (p5)<-[:" + Rels.POST_HAS_CREATOR + "]-(post0),\n"
                   + " (p6)<-[:" + Rels.POST_HAS_CREATOR + "]-(post1),\n"
                   + " (p6)<-[:" + Rels.POST_HAS_CREATOR + "]-(post2),\n"
                   + " (p7)<-[:" + Rels.POST_HAS_CREATOR + "]-(post3),\n"

                   // Person HAS_CREATOR Comment
                   + " (p7)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment1),\n"
                   + " (p8)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment2),\n"
                   + " (p8)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment3),\n"
                   + " (p9)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment4),\n"
                   + " (p9)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment5),\n"
                   + " (p10)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment6),\n"
                   + " (p0)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment7),\n"
                   + " (p1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment8),\n"
                   + " (p1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment9),\n"
                   + " (p2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment10),\n"
                   + " (p2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment11),\n"
                   + " (p3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment12),\n"
                   + " (p3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment13),\n"
                   + " (p3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment14),\n"
                   + " (p3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment15),\n"
                   + " (p4)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment16),\n"
                   + " (p4)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment17),\n"
                   + " (p4)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment18),\n"

                   // Comment REPLY_OF Post
                   + " (comment1)-[:" + Rels.REPLY_OF_POST + "]->(post0),\n"
                   + " (comment2)-[:" + Rels.REPLY_OF_POST + "]->(post0),\n"
                   + " (comment3)-[:" + Rels.REPLY_OF_POST + "]->(post0),\n"
                   + " (comment4)-[:" + Rels.REPLY_OF_POST + "]->(post0),\n"
                   + " (comment6)-[:" + Rels.REPLY_OF_POST + "]->(post0),\n"
                   + " (comment8)-[:" + Rels.REPLY_OF_POST + "]->(post0),\n"
                   + " (comment7)-[:" + Rels.REPLY_OF_POST + "]->(post1),\n"
                   + " (comment9)-[:" + Rels.REPLY_OF_POST + "]->(post2),\n"
                   + " (comment10)-[:" + Rels.REPLY_OF_POST + "]->(post3),\n"
                   + " (comment11)-[:" + Rels.REPLY_OF_POST + "]->(post3),\n"

                   // Comment REPLY_OF Comment
                   + " (comment12)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment1),\n"
                   + " (comment13)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment2),\n"
                   + " (comment14)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment2),\n"
                   + " (comment16)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment3),\n"
                   + " (comment17)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment4),\n"
                   + " (comment5)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment6),\n"
                   + " (comment18)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment6),\n"
                   + " (comment15)-[:" + Rels.REPLY_OF_COMMENT + "]->(comment16)\n"

                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query20GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // TagClass
                   + " (tagClass1:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass1'}),\n"
                   + " (tagClass11:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass11'}),\n"
                   + " (tagClass2:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass2'}),\n"
                   + " (tagClass3:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass3'}),\n"
                   + " (tagClass31:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass31'}),\n"
                   + " (tagClass311:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass311'}),\n"
                   + " (tagClass4:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass4'}),\n"
                   + " (tagClass5:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass5'}),\n"
                   + " (tagClass6:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass6'}),\n"

                   // Tag
                   + " (tag11:" + Nodes.Tag + " {" + Tag.NAME + ":'tag11'}),\n"
                   + " (tag12:" + Nodes.Tag + " {" + Tag.NAME + ":'tag12'}),\n"
                   + " (tag13:" + Nodes.Tag + " {" + Tag.NAME + ":'tag13'}),\n"
                   + " (tag21:" + Nodes.Tag + " {" + Tag.NAME + ":'tag21'}),\n"
                   + " (tag22:" + Nodes.Tag + " {" + Tag.NAME + ":'tag22'}),\n"
                   + " (tag31:" + Nodes.Tag + " {" + Tag.NAME + ":'tag31'}),\n"
                   + " (tag41:" + Nodes.Tag + " {" + Tag.NAME + ":'tag41'}),\n"
                   + " (tag42:" + Nodes.Tag + " {" + Tag.NAME + ":'tag42'}),\n"
                   + " (tag51:" + Nodes.Tag + " {" + Tag.NAME + ":'tag51'}),\n"
                   + " (tag61:" + Nodes.Tag + " {" + Tag.NAME + ":'tag61'}),\n"

                   // Post
                   + " (post1:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":1}),\n"
                   + " (post2:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":2}),\n"
                   + " (post3:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":3}),\n"
                   + " (post4:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":4}),\n"
                   + " (post5:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":5}),\n"
                   + " (post6:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":6}),\n"
                   + " (post7:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":7}),\n"
                   + " (post8:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":8}),\n"

                   // Comment
                   + " (comment9:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":9}),\n"
                   + " (comment10:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":10}),\n"
                   + " (comment11:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":11}),\n"
                   + " (comment12:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":12}),\n"
                   + " (comment13:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":13}),\n"
                   + " (comment14:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":14}),\n"
                   + " (comment15:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":15}),\n"
                   + " (comment16:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":16}),\n"

                   // TagClass IS_SUBCLASS TagClass
                   + " (tagClass11)-[:" + Rels.IS_SUBCLASS_OF + "]->(tagClass1),\n"
                   + " (tagClass31)-[:" + Rels.IS_SUBCLASS_OF + "]->(tagClass3),\n"
                   + " (tagClass311)-[:" + Rels.IS_SUBCLASS_OF + "]->(tagClass31),\n"

                   // Tag HAS_TYPE TagClass
                   + " (tag11)-[:" + Rels.HAS_TYPE + "]->(tagClass11),\n"
                   + " (tag12)-[:" + Rels.HAS_TYPE + "]->(tagClass1),\n"
                   + " (tag13)-[:" + Rels.HAS_TYPE + "]->(tagClass1),\n"
                   + " (tag21)-[:" + Rels.HAS_TYPE + "]->(tagClass2),\n"
                   + " (tag22)-[:" + Rels.HAS_TYPE + "]->(tagClass2),\n"
                   + " (tag31)-[:" + Rels.HAS_TYPE + "]->(tagClass311),\n"
                   + " (tag41)-[:" + Rels.HAS_TYPE + "]->(tagClass4),\n"
                   + " (tag42)-[:" + Rels.HAS_TYPE + "]->(tagClass4),\n"
                   + " (tag51)-[:" + Rels.HAS_TYPE + "]->(tagClass5),\n"
                   + " (tag61)-[:" + Rels.HAS_TYPE + "]->(tagClass6),\n"

                   // Post HAS_TAG Tag
                   + " (post1)-[:" + Rels.POST_HAS_TAG + "]->(tag11),\n"
                   + " (post1)-[:" + Rels.POST_HAS_TAG + "]->(tag12),\n"
                   + " (post1)-[:" + Rels.POST_HAS_TAG + "]->(tag13),\n"
                   + " (post2)-[:" + Rels.POST_HAS_TAG + "]->(tag21),\n"
                   + " (post3)-[:" + Rels.POST_HAS_TAG + "]->(tag31),\n"
                   + " (post4)-[:" + Rels.POST_HAS_TAG + "]->(tag11),\n"
                   + " (post5)-[:" + Rels.POST_HAS_TAG + "]->(tag22),\n"
                   + " (post6)-[:" + Rels.POST_HAS_TAG + "]->(tag31),\n"
                   + " (post7)-[:" + Rels.POST_HAS_TAG + "]->(tag41),\n"
                   + " (post8)-[:" + Rels.POST_HAS_TAG + "]->(tag42),\n"

                   // Comment HAS_TAG Tag
                   + " (comment9)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag11),\n"
                   + " (comment10)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag12),\n"
                   + " (comment11)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag12),\n"
                   + " (comment12)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag21),\n"
                   + " (comment12)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag22),\n"
                   + " (comment13)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag22),\n"
                   + " (comment13)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag31),\n"
                   + " (comment14)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag31),\n"
                   + " (comment14)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag42),\n"
                   + " (comment15)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag51),\n"
                   + " (comment16)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag51)\n"

                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query21GraphMaker extends QueryGraphMaker
    {
        static final long END_DATE = date( 2001, 1, 30 );

        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Country
                   + " (country1:" + Place.Type.Country + " {" + Place.NAME + ":'country1'}),\n"
                   + " (country2:" + Place.Type.Country + " {" + Place.NAME + ":'country2'}),\n"

                   // City
                   + " (city1:" + Place.Type.Country + " {" + Place.NAME + ":'city1'}),\n"
                   + " (city2:" + Place.Type.Country + " {" + Place.NAME + ":'city2'}),\n"

                   // Person
                   + " (person1:" + Nodes.Person + " {"
                   + Person.ID + ":1,"
                   + Person.CREATION_DATE + ":" + (END_DATE - TimeUnit.DAYS.toMillis( 200 ))
                   + "}),\n"
                   + " (person2:" + Nodes.Person + " {"
                   + Person.ID + ":2,"
                   + Person.CREATION_DATE + ":" + (END_DATE - TimeUnit.DAYS.toMillis( 201 ))
                   + "}),\n"
                   + " (person3:" + Nodes.Person + " {"
                   + Person.ID + ":3,"
                   + Person.CREATION_DATE + ":" + (END_DATE - TimeUnit.DAYS.toMillis( 201 ))
                   + "}),\n"
                   + " (person4:" + Nodes.Person + " {"
                   + Person.ID + ":4,"
                   + Person.CREATION_DATE + ":" + (END_DATE - TimeUnit.DAYS.toMillis( 201 ))
                   + "}),\n"
                   + " (person5:" + Nodes.Person + " {"
                   + Person.ID + ":5,"
                   + Person.CREATION_DATE + ":" + (END_DATE - TimeUnit.DAYS.toMillis( 201 ))
                   + "}),\n"
                   + " (person6:" + Nodes.Person + " {"
                   + Person.ID + ":6,"
                   + Person.CREATION_DATE + ":" + (END_DATE - TimeUnit.DAYS.toMillis( 201 ))
                   + "}),\n"
                   + " (person7:" + Nodes.Person + " {"
                   + Person.ID + ":7,"
                   + Person.CREATION_DATE + ":" + (END_DATE - TimeUnit.DAYS.toMillis( 201 ))
                   + "}),\n"
                   + " (person8:" + Nodes.Person + " {"
                   + Person.ID + ":8,"
                   + Person.CREATION_DATE + ":" + (END_DATE - TimeUnit.DAYS.toMillis( 89 ))
                   + "}),\n"
                   + " (person9:" + Nodes.Person + " {"
                   + Person.ID + ":9,"
                   + Person.CREATION_DATE + ":" + (END_DATE - TimeUnit.DAYS.toMillis( 60 ))
                   + "}),\n"
                   + " (person10:" + Nodes.Person + " {"
                   + Person.ID + ":10,"
                   + Person.CREATION_DATE + ":" + (END_DATE - TimeUnit.DAYS.toMillis( 200 ))
                   + "}),\n"
                   + " (person11:" + Nodes.Person + " {"
                   + Person.ID + ":11,"
                   + Person.CREATION_DATE + ":" + (END_DATE - TimeUnit.DAYS.toMillis( 60 ))
                   + "}),\n"
                   + " (person12:" + Nodes.Person + " {"
                   + Person.ID + ":12,"
                   + Person.CREATION_DATE + ":" + (END_DATE - TimeUnit.DAYS.toMillis( 30 ))
                   + "}),\n"
                   + " (person13:" + Nodes.Person + " {"
                   + Person.ID + ":13,"
                   + Person.CREATION_DATE + ":" + (END_DATE - TimeUnit.DAYS.toMillis( 31 ))
                   + "}),\n"

                   // Comment
                   + " (comment1:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":1}),\n"
                   + " (comment2:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":2}),\n"
                   + " (comment3:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":3}),\n"
                   + " (comment4:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":4}),\n"
                   + " (comment5:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":5}),\n"
                   + " (comment6:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":6}),\n"
                   + " (comment7:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":7}),\n"
                   + " (comment8:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":8}),\n"
                   + " (comment9:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":9}),\n"

                   // Post
                   + " (post10:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":10}),\n"
                   + " (post11:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":11}),\n"
                   + " (post12:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":12}),\n"
                   + " (post13:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":13}),\n"
                   + " (post14:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":14}),\n"
                   + " (post15:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":15}),\n"
                   + " (post16:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":16}),\n"
                   + " (post17:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":17}),\n"
                   + " (post18:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":18}),\n"
                   + " (post19:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":19}),\n"
                   + " (post20:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":20}),\n"
                   + " (post21:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":21}),\n"
                   + " (post22:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":22}),\n"

                   // Country IS_PART_OF City
                   + " (country1)<-[:" + Rels.IS_PART_OF + "]-(city1),\n"
                   + " (country2)<-[:" + Rels.IS_PART_OF + "]-(city2),\n"

                   // City IS_LOCATED_IN Person
                   + " (city1)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person1),\n"
                   + " (city1)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person2),\n"
                   + " (city1)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person3),\n"
                   + " (city1)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person4),\n"
                   + " (city1)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person5),\n"
                   + " (city1)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person6),\n"
                   + " (city1)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person13),\n"
                   + " (city2)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person7),\n"
                   + " (city2)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person8),\n"
                   + " (city2)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person9),\n"
                   + " (city2)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person10),\n"
                   + " (city2)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person11),\n"
                   + " (city2)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person12),\n"

                   // Person HAS_CREATOR Post
                   + " (person5)<-[:" + Rels.POST_HAS_CREATOR + "]-(post10),\n"
                   + " (person7)<-[:" + Rels.POST_HAS_CREATOR + "]-(post11),\n"
                   + " (person8)<-[:" + Rels.POST_HAS_CREATOR + "]-(post12),\n"
                   + " (person8)<-[:" + Rels.POST_HAS_CREATOR + "]-(post13),\n"
                   + " (person8)<-[:" + Rels.POST_HAS_CREATOR + "]-(post14),\n"
                   + " (person10)<-[:" + Rels.POST_HAS_CREATOR + "]-(post15),\n"
                   + " (person10)<-[:" + Rels.POST_HAS_CREATOR + "]-(post16),\n"
                   + " (person10)<-[:" + Rels.POST_HAS_CREATOR + "]-(post17),\n"
                   + " (person11)<-[:" + Rels.POST_HAS_CREATOR + "]-(post18),\n"
                   + " (person11)<-[:" + Rels.POST_HAS_CREATOR + "]-(post19),\n"
                   + " (person12)<-[:" + Rels.POST_HAS_CREATOR + "]-(post20),\n"
                   + " (person13)<-[:" + Rels.POST_HAS_CREATOR + "]-(post21),\n"
                   + " (person13)<-[:" + Rels.POST_HAS_CREATOR + "]-(post22),\n"

                   // Person HAS_CREATOR Comment
                   + " (person1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment1),\n"
                   + " (person2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment2),\n"
                   + " (person3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment3),\n"
                   + " (person4)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment4),\n"
                   + " (person5)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment5),\n"
                   + " (person6)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment6),\n"
                   + " (person9)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment7),\n"
                   + " (person9)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment8),\n"
                   + " (person10)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment9),\n"

                   // Post LIKES Person
                   + " (post10)<-[:" + Rels.LIKES_POST + "]-(person13),\n"

                   // Comment LIKES Person
                   + " (comment1)<-[:" + Rels.LIKES_COMMENT + "]-(person8),\n"
                   + " (comment2)<-[:" + Rels.LIKES_COMMENT + "]-(person9),\n"
                   + " (comment2)<-[:" + Rels.LIKES_COMMENT + "]-(person10),\n"
                   + " (comment3)<-[:" + Rels.LIKES_COMMENT + "]-(person9),\n"
                   + " (comment3)<-[:" + Rels.LIKES_COMMENT + "]-(person10),\n"
                   + " (comment3)<-[:" + Rels.LIKES_COMMENT + "]-(person11),\n"
                   + " (comment3)<-[:" + Rels.LIKES_COMMENT + "]-(person12),\n"
                   + " (comment4)<-[:" + Rels.LIKES_COMMENT + "]-(person8),\n"
                   + " (comment4)<-[:" + Rels.LIKES_COMMENT + "]-(person11),\n"
                   + " (comment4)<-[:" + Rels.LIKES_COMMENT + "]-(person12),\n"
                   + " (comment4)<-[:" + Rels.LIKES_COMMENT + "]-(person13),\n"
                   + " (comment5)<-[:" + Rels.LIKES_COMMENT + "]-(person8),\n"
                   + " (comment5)<-[:" + Rels.LIKES_COMMENT + "]-(person13)\n"

                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query22GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Country
                   + " (country1:" + Place.Type.Country + " {" + Place.NAME + ":'country1'}),\n"
                   + " (country2:" + Place.Type.Country + " {" + Place.NAME + ":'country2'}),\n"
                   + " (country3:" + Place.Type.Country + " {" + Place.NAME + ":'country3'}),\n"

                   // City
                   + " (city11:" + Place.Type.City + " {" + Place.NAME + ":'city11'}),\n"
                   + " (city12:" + Place.Type.City + " {" + Place.NAME + ":'city12'}),\n"
                   + " (city13:" + Place.Type.City + " {" + Place.NAME + ":'city13'}),\n"
                   + " (city21:" + Place.Type.City + " {" + Place.NAME + ":'city21'}),\n"
                   + " (city22:" + Place.Type.City + " {" + Place.NAME + ":'city22'}),\n"
                   + " (city31:" + Place.Type.City + " {" + Place.NAME + ":'city31'}),\n"

                   // Person
                   + " (person11:" + Nodes.Person + " {" + Person.ID + ":11}),\n"
                   + " (person12:" + Nodes.Person + " {" + Person.ID + ":12}),\n"
                   + " (person13:" + Nodes.Person + " {" + Person.ID + ":13}),\n"
                   + " (person14:" + Nodes.Person + " {" + Person.ID + ":14}),\n"
                   + " (person15:" + Nodes.Person + " {" + Person.ID + ":15}),\n"
                   + " (person21:" + Nodes.Person + " {" + Person.ID + ":21}),\n"
                   + " (person22:" + Nodes.Person + " {" + Person.ID + ":22}),\n"
                   + " (person23:" + Nodes.Person + " {" + Person.ID + ":23}),\n"
                   + " (person31:" + Nodes.Person + " {" + Person.ID + ":31}),\n"
                   + " (person32:" + Nodes.Person + " {" + Person.ID + ":32}),\n"

                   // Post
                   + " (post1:" + Nodes.Post + ":" + Nodes.Message + " {" + Message.ID + ":1}),\n"

                   // Comment
                   + " (comment10:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":10}),\n"
                   + " (comment11:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":11}),\n"
                   + " (comment12:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":12}),\n"
                   + " (comment13:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":13}),\n"
                   + " (comment14:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":14}),\n"
                   + " (comment15:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":15}),\n"
                   + " (comment16:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":16}),\n"
                   + " (comment17:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":17}),\n"
                   + " (comment18:" + Nodes.Comment + ":" + Nodes.Message + " {" + Message.ID + ":18}),\n"

                   // Country IS_PART_OF City
                   + " (country1)<-[:" + Rels.IS_PART_OF + "]-(city11),\n"
                   + " (country1)<-[:" + Rels.IS_PART_OF + "]-(city12),\n"
                   + " (country1)<-[:" + Rels.IS_PART_OF + "]-(city13),\n"
                   + " (country2)<-[:" + Rels.IS_PART_OF + "]-(city21),\n"
                   + " (country2)<-[:" + Rels.IS_PART_OF + "]-(city22),\n"
                   + " (country3)<-[:" + Rels.IS_PART_OF + "]-(city31),\n"

                   // City IS_LOCATED_IN Person
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person11),\n"
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person12),\n"
                   + " (city12)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person13),\n"
                   + " (city12)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person14),\n"
                   + " (city13)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person15),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person21),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person22),\n"
                   + " (city22)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person23),\n"
                   + " (city31)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person31),\n"
                   + " (city31)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person32),\n"

                   // Person KNOWS Person
                   + " (person11)<-[:" + Rels.KNOWS + "]-(person21),\n"
                   + " (person11)<-[:" + Rels.KNOWS + "]-(person22),\n"
                   + " (person12)<-[:" + Rels.KNOWS + "]-(person31),\n"
                   + " (person13)<-[:" + Rels.KNOWS + "]-(person22),\n"
                   + " (person13)<-[:" + Rels.KNOWS + "]-(person23),\n"
                   + " (person14)<-[:" + Rels.KNOWS + "]-(person23),\n"

                   // Person HAS_CREATOR Post
                   + " (person21)<-[:" + Rels.POST_HAS_CREATOR + "]-(post1),\n"

                   // Person HAS_CREATOR Comment
                   + " (person21)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment10),\n"
                   + " (person11)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment11),\n"
                   + " (person11)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment12),\n"
                   + " (person21)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment13),\n"
                   + " (person31)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment14),\n"
                   + " (person31)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment15),\n"
                   + " (person31)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment16),\n"
                   + " (person31)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment17),\n"
                   + " (person31)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment18),\n"

                   // Post LIKED Person
                   + " (post1)<-[:" + Rels.LIKES_POST + "]-(person11),\n"

                   // Comment LIKED Person
                   + " (comment11)<-[:" + Rels.LIKES_COMMENT + "]-(person21),\n"
                   + " (comment12)<-[:" + Rels.LIKES_COMMENT + "]-(person21),\n"
                   + " (comment13)<-[:" + Rels.LIKES_COMMENT + "]-(person11),\n"
                   + " (comment14)<-[:" + Rels.LIKES_COMMENT + "]-(person12),\n"
                   + " (comment15)<-[:" + Rels.LIKES_COMMENT + "]-(person12),\n"
                   + " (comment16)<-[:" + Rels.LIKES_COMMENT + "]-(person12),\n"
                   + " (comment17)<-[:" + Rels.LIKES_COMMENT + "]-(person12),\n"
                   + " (comment18)<-[:" + Rels.LIKES_COMMENT + "]-(person12),\n"

                   // Post REPLY_OF Comment
                   + " (post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment11),\n"

                   // Comment REPLY_OF Comment
                   + " (comment10)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment12),\n"
                   + " (comment11)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment10),\n"
                   + " (comment12)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment13)\n"

                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query23GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   // Country
                   + " (country1:" + Place.Type.Country + " {" + Place.NAME + ":'country1'}),\n"
                   + " (country2:" + Place.Type.Country + " {" + Place.NAME + ":'country2'}),\n"
                   + " (country3:" + Place.Type.Country + " {" + Place.NAME + ":'country3'}),\n"
                   + " (country4:" + Place.Type.Country + " {" + Place.NAME + ":'country4'}),\n"

                   // City
                   + " (city11:" + Place.Type.City + " {" + Place.NAME + ":'city11'}),\n"
                   + " (city21:" + Place.Type.City + " {" + Place.NAME + ":'city21'}),\n"
                   + " (city22:" + Place.Type.City + " {" + Place.NAME + ":'city22'}),\n"
                   + " (city31:" + Place.Type.City + " {" + Place.NAME + ":'city31'}),\n"
                   + " (city41:" + Place.Type.City + " {" + Place.NAME + ":'city41'}),\n"

                   // Person
                   + " (person11:" + Nodes.Person + " {" + Person.ID + ":11}),\n"
                   + " (person12:" + Nodes.Person + " {" + Person.ID + ":12}),\n"
                   + " (person13:" + Nodes.Person + " {" + Person.ID + ":13}),\n"
                   + " (person14:" + Nodes.Person + " {" + Person.ID + ":14}),\n"
                   + " (person15:" + Nodes.Person + " {" + Person.ID + ":15}),\n"
                   + " (person21:" + Nodes.Person + " {" + Person.ID + ":21}),\n"
                   + " (person22:" + Nodes.Person + " {" + Person.ID + ":22}),\n"
                   + " (person41:" + Nodes.Person + " {" + Person.ID + ":41}),\n"

                   // Post
                   + " (post1:" + Nodes.Post + ":" + Nodes.Message + " {"
                   + Message.ID + ":1,"
                   + Message.CREATION_DATE + ":" + date( 1982, 1, 23 )
                   + "}),\n"
                   + " (post2:" + Nodes.Post + ":" + Nodes.Message + " {"
                   + Message.ID + ":2,"
                   + Message.CREATION_DATE + ":" + date( 2021, 1, 1 )
                   + "}),\n"
                   + " (post3:" + Nodes.Post + ":" + Nodes.Message + " {"
                   + Message.ID + ":3,"
                   + Message.CREATION_DATE + ":" + date( 2000, 2, 10 )
                   + "}),\n"
                   + " (post4:" + Nodes.Post + ":" + Nodes.Message + " {"
                   + Message.ID + ":4,"
                   + Message.CREATION_DATE + ":" + date( 2010, 2, 25 )
                   + "}),\n"
                   + " (post5:" + Nodes.Post + ":" + Nodes.Message + " {"
                   + Message.ID + ":5,"
                   + Message.CREATION_DATE + ":" + date( 2001, 2, 2 )
                   + "}),\n"
                   + " (post6:" + Nodes.Post + ":" + Nodes.Message + " {"
                   + Message.ID + ":6,"
                   + Message.CREATION_DATE + ":" + date( 1999, 1, 30 )
                   + "}),\n"

                   // Comment
                   + " (comment7:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.ID + ":7,"
                   + Message.CREATION_DATE + ":" + date( 1977, 1, 1 )
                   + "}),\n"
                   + " (comment8:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.ID + ":8,"
                   + Message.CREATION_DATE + ":" + date( 2000, 1, 20 )
                   + "}),\n"
                   + " (comment9:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.ID + ":9,"
                   + Message.CREATION_DATE + ":" + date( 2050, 1, 5 )
                   + "}),\n"
                   + " (comment10:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.ID + ":10,"
                   + Message.CREATION_DATE + ":" + date( 1980, 3, 2 )
                   + "}),\n"
                   + " (comment11:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.ID + ":11,"
                   + Message.CREATION_DATE + ":" + date( 2007, 4, 28 )
                   + "}),\n"
                   + " (comment12:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.ID + ":12,"
                   + Message.CREATION_DATE + ":" + date( 1974, 3, 12 )
                   + "}),\n"

                   // Country IS PART OF City
                   + " (country1)<-[:" + Rels.IS_PART_OF + "]-(city11),\n"
                   + " (country2)<-[:" + Rels.IS_PART_OF + "]-(city21),\n"
                   + " (country2)<-[:" + Rels.IS_PART_OF + "]-(city22),\n"
                   + " (country3)<-[:" + Rels.IS_PART_OF + "]-(city31),\n"
                   + " (country4)<-[:" + Rels.IS_PART_OF + "]-(city41),\n"

                   // Person HAS CREATOR Post
                   + " (person11)<-[:" + Rels.POST_HAS_CREATOR + "]-(post1),\n"
                   + " (person11)<-[:" + Rels.POST_HAS_CREATOR + "]-(post2),\n"
                   + " (person12)<-[:" + Rels.POST_HAS_CREATOR + "]-(post3),\n"
                   + " (person14)<-[:" + Rels.POST_HAS_CREATOR + "]-(post4),\n"
                   + " (person15)<-[:" + Rels.POST_HAS_CREATOR + "]-(post5),\n"
                   + " (person15)<-[:" + Rels.POST_HAS_CREATOR + "]-(post6),\n"

                   // Person HAS CREATOR Comment
                   + " (person15)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment7),\n"
                   + " (person15)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment8),\n"
                   + " (person15)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment9),\n"
                   + " (person15)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment10),\n"
                   + " (person15)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment11),\n"
                   + " (person41)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment12),\n"

                   // City IS LOCATED IN Person
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person11),\n"
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person12),\n"
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person13),\n"
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person14),\n"
                   + " (city11)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person15),\n"
                   + " (city21)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person21),\n"
                   + " (city22)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person22),\n"
                   + " (city41)<-[:" + Rels.PERSON_IS_LOCATED_IN + "]-(person41),\n"

                   // Country IS LOCATED IN Post
                   + " (country1)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post1),\n"
                   + " (country2)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post2),\n"
                   + " (country2)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post3),\n"
                   + " (country2)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post4),\n"
                   + " (country3)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post5),\n"
                   + " (country3)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post6),\n"

                   // Country IS LOCATED IN Comment
                   + " (country3)<-[:" + Rels.COMMENT_IS_LOCATED_IN + "]-(comment7),\n"
                   + " (country4)<-[:" + Rels.COMMENT_IS_LOCATED_IN + "]-(comment8),\n"
                   + " (country4)<-[:" + Rels.COMMENT_IS_LOCATED_IN + "]-(comment9),\n"
                   + " (country4)<-[:" + Rels.COMMENT_IS_LOCATED_IN + "]-(comment10),\n"
                   + " (country4)<-[:" + Rels.COMMENT_IS_LOCATED_IN + "]-(comment11),\n"
                   + " (country4)<-[:" + Rels.COMMENT_IS_LOCATED_IN + "]-(comment12)\n"

                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query24GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"

                   // Continent
                   + " (continent1:" + Place.Type.Continent + " {" + Place.NAME + ":'continent1'}),\n"
                   + " (continent2:" + Place.Type.Continent + " {" + Place.NAME + ":'continent2'}),\n"
                   + " (continent3:" + Place.Type.Continent + " {" + Place.NAME + ":'continent3'}),\n"

                   // Country
                   + " (country11:" + Place.Type.Country + " {" + Place.NAME + ":'country11'}),\n"
                   + " (country12:" + Place.Type.Country + " {" + Place.NAME + ":'country12'}),\n"
                   + " (country21:" + Place.Type.Country + " {" + Place.NAME + ":'country21'}),\n"
                   + " (country31:" + Place.Type.Country + " {" + Place.NAME + ":'country31'}),\n"
                   + " (country32:" + Place.Type.Country + " {" + Place.NAME + ":'country32'}),\n"

                   // TagClass
                   + " (tagClass1:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass1'}),\n"
                   + " (tagClass11:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass11'}),\n"
                   + " (tagClass2:" + Nodes.TagClass + " {" + TagClass.NAME + ":'tagClass2'}),\n"

                   // Tag
                   + " (tag111:" + Nodes.Tag + " {" + Tag.NAME + ":'tag111'}),\n"
                   + " (tag11:" + Nodes.Tag + " {" + Tag.NAME + ":'tag11'}),\n"
                   + " (tag12:" + Nodes.Tag + " {" + Tag.NAME + ":'tag12'}),\n"
                   + " (tag13:" + Nodes.Tag + " {" + Tag.NAME + ":'tag13'}),\n"
                   + " (tag21:" + Nodes.Tag + " {" + Tag.NAME + ":'tag21'}),\n"

                   // Person
                   + " (person1:" + Nodes.Person + " {" + Person.ID + ":1}),\n"
                   + " (person2:" + Nodes.Person + " {" + Person.ID + ":2}),\n"
                   + " (person3:" + Nodes.Person + " {" + Person.ID + ":3}),\n"

                   // Post
                   + " (post1:" + Nodes.Post + ":" + Nodes.Message + " {"
                   + Message.ID + ":1,"
                   + Message.CREATION_DATE + ":" + date( 2000, 1 )
                   + "}),\n"
                   + " (post2:" + Nodes.Post + ":" + Nodes.Message + " {"
                   + Message.ID + ":2,"
                   + Message.CREATION_DATE + ":" + date( 2000, 1 )
                   + "}),\n"
                   + " (post3:" + Nodes.Post + ":" + Nodes.Message + " {"
                   + Message.ID + ":3,"
                   + Message.CREATION_DATE + ":" + date( 2000, 1 )
                   + "}),\n"
                   + " (post4:" + Nodes.Post + ":" + Nodes.Message + " {"
                   + Message.ID + ":4,"
                   + Message.CREATION_DATE + ":" + date( 2000, 1 )
                   + "}),\n"
                   + " (post5:" + Nodes.Post + ":" + Nodes.Message + " {"
                   + Message.ID + ":5,"
                   + Message.CREATION_DATE + ":" + date( 2000, 1 )
                   + "}),\n"

                   // Comment
                   + " (comment6:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.ID + ":6,"
                   + Message.CREATION_DATE + ":" + date( 2000, 2 )
                   + "}),\n"
                   + " (comment7:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.ID + ":7,"
                   + Message.CREATION_DATE + ":" + date( 2000, 2 )
                   + "}),\n"
                   + " (comment8:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.ID + ":8,"
                   + Message.CREATION_DATE + ":" + date( 2000, 2 )
                   + "}),\n"

                   // TagClass IS SUB CLASS TagClass
                   + " (tagClass1)<-[:" + Rels.IS_SUBCLASS_OF + "]-(tagClass11),\n"

                   // TagClass HAS TYPE Tag
                   + " (tagClass11)<-[:" + Rels.HAS_TYPE + "]-(tag111),\n"
                   + " (tagClass1)<-[:" + Rels.HAS_TYPE + "]-(tag11),\n"
                   + " (tagClass1)<-[:" + Rels.HAS_TYPE + "]-(tag12),\n"
                   + " (tagClass1)<-[:" + Rels.HAS_TYPE + "]-(tag13),\n"
                   + " (tagClass1)<-[:" + Rels.HAS_TYPE + "]-(tag14),\n"
                   + " (tagClass2)<-[:" + Rels.HAS_TYPE + "]-(tag21),\n"

                   // Continent IS PART OF Country
                   + " (continent1)<-[:" + Rels.IS_PART_OF + "]-(country11),\n"
                   + " (continent1)<-[:" + Rels.IS_PART_OF + "]-(country12),\n"
                   + " (continent2)<-[:" + Rels.IS_PART_OF + "]-(country21),\n"
                   + " (continent3)<-[:" + Rels.IS_PART_OF + "]-(country31),\n"
                   + " (continent3)<-[:" + Rels.IS_PART_OF + "]-(country32),\n"

                   // Post LIKED Person
                   + " (post3)<-[:" + Rels.LIKES_POST + "]-(person1),\n"
                   + " (post4)<-[:" + Rels.LIKES_POST + "]-(person1),\n"
                   + " (post4)<-[:" + Rels.LIKES_POST + "]-(person2),\n"

                   // Comment LIKED Person
                   + " (comment7)<-[:" + Rels.LIKES_COMMENT + "]-(person3),\n"

                   // Tag HAS TAG Post
                   + " (tag111)<-[:" + Rels.POST_HAS_TAG + "]-(post1),\n"
                   + " (tag11)<-[:" + Rels.POST_HAS_TAG + "]-(post1),\n"
                   + " (tag12)<-[:" + Rels.POST_HAS_TAG + "]-(post1),\n"
                   + " (tag13)<-[:" + Rels.POST_HAS_TAG + "]-(post2),\n"
                   + " (tag111)<-[:" + Rels.POST_HAS_TAG + "]-(post3),\n"
                   + " (tag13)<-[:" + Rels.POST_HAS_TAG + "]-(post4),\n"
                   + " (tag13)<-[:" + Rels.POST_HAS_TAG + "]-(post5),\n"

                   // Tag HAS TAG Comment
                   + " (tag13)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment6),\n"
                   + " (tag13)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment7),\n"
                   + " (tag21)<-[:" + Rels.COMMENT_HAS_TAG + "]-(comment8),\n"

                   // Country LOCATED IN Post
                   + " (country11)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post1),\n"
                   + " (country11)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post2),\n"
                   + " (country31)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post3),\n"
                   + " (country21)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post4),\n"
                   + " (country21)<-[:" + Rels.POST_IS_LOCATED_IN + "]-(post5),\n"

                   // Country LOCATED IN Comment
                   + " (country12)<-[:" + Rels.COMMENT_IS_LOCATED_IN + "]-(comment6),\n"
                   + " (country21)<-[:" + Rels.COMMENT_IS_LOCATED_IN + "]-(comment7),\n"
                   + " (country32)<-[:" + Rels.COMMENT_IS_LOCATED_IN + "]-(comment8)\n"

                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }
}
