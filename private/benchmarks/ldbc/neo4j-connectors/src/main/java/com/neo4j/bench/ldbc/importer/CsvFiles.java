/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import java.util.regex.Pattern;

public class CsvFiles
{
    /*
     * Nodes
     */
    public static final Pattern COMMENT = Pattern.compile( "comment_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern POST = Pattern.compile( "post_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PERSON = Pattern.compile( "person_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern FORUM = Pattern.compile( "forum_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern TAG = Pattern.compile( "tag_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern TAGCLASS = Pattern.compile( "tagclass_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern ORGANIZATION = Pattern.compile( "organisation_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PLACE = Pattern.compile( "place_\\d{1,2}_\\d{1,2}.csv" );

    /*
     * Relationships
     */
    public static final Pattern COMMENT_REPLY_OF_COMMENT =
            Pattern.compile( "comment_replyOf_comment_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern COMMENT_REPLY_OF_POST =
            Pattern.compile( "comment_replyOf_post_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern COMMENT_LOCATED_IN_PLACE =
            Pattern.compile( "comment_isLocatedIn_place_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PLACE_IS_PART_OF_PLACE =
            Pattern.compile( "place_isPartOf_place_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PERSON_KNOWS_PERSON =
            Pattern.compile( "person_knows_person_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PERSON_STUDIES_AT_ORGANISATION =
            Pattern.compile( "person_studyAt_organisation_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern COMMENT_HAS_CREATOR_PERSON =
            Pattern.compile( "comment_hasCreator_person_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern POST_HAS_CREATOR_PERSON =
            Pattern.compile( "post_hasCreator_person_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern FORUM_HAS_MODERATOR_PERSON =
            Pattern.compile( "forum_hasModerator_person_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PERSON_IS_LOCATED_IN_PLACE =
            Pattern.compile( "person_isLocatedIn_place_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PERSON_WORKS_AT_ORGANISATION =
            Pattern.compile( "person_workAt_organisation_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PERSON_HAS_INTEREST_TAG =
            Pattern.compile( "person_hasInterest_tag_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern POST_HAS_TAG_TAG =
            Pattern.compile( "post_hasTag_tag_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PERSON_LIKES_POST =
            Pattern.compile( "person_likes_post_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern POST_IS_LOCATED_IN_PLACE =
            Pattern.compile( "post_isLocatedIn_place_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern FORUM_HAS_MEMBER_PERSON =
            Pattern.compile( "forum_hasMember_person_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern FORUMS_CONTAINER_OF_POST =
            Pattern.compile( "forum_containerOf_post_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern FORUM_HAS_TAG_TAG =
            Pattern.compile( "forum_hasTag_tag_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern TAG_HAS_TYPE_TAGCLASS =
            Pattern.compile( "tag_hasType_tagclass_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern TAGCLASS_IS_SUBCLASS_OF_TAGCLASS =
            Pattern.compile( "tagclass_isSubclassOf_tagclass_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern ORGANISATION_IS_LOCATED_IN_PLACE =
            Pattern.compile( "organisation_isLocatedIn_place_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PERSON_LIKES_COMMENT =
            Pattern.compile( "person_likes_comment_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern COMMENT_HAS_TAG_TAG =
            Pattern.compile( "comment_hasTag_tag_\\d{1,2}_\\d{1,2}.csv" );
}
