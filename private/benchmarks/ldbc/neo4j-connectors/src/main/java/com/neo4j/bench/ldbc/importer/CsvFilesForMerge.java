/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import java.util.regex.Pattern;

import static java.lang.String.format;

public class CsvFilesForMerge
{
    public static final String NUM_REGEX = "\\d{1,2}";

    /*
     * Nodes
     */
    public static final Pattern COMMENT = numPattern( "comment" );
    public static final Pattern POST = numPattern( "post" );
    public static final Pattern PERSON = numPattern( "person" );
    public static final Pattern FORUM = numPattern( "forum" );
    public static final Pattern TAG = numPattern( "tag" );
    public static final Pattern TAGCLASS = numPattern( "tagclass" );
    public static final Pattern ORGANIZATION = numPattern( "organisation" );
    public static final Pattern PLACE = numPattern( "place" );

    /*
     * Relationships
     */
    public static final Pattern PERSON_KNOWS_PERSON = numPattern( "person_knows_person" );
    public static final Pattern PERSON_STUDIES_AT_ORGANISATION = numPattern( "person_studyAt_organisation" );
    public static final Pattern PERSON_WORKS_AT_ORGANISATION = numPattern( "person_workAt_organisation" );
    public static final Pattern PERSON_HAS_INTEREST_TAG = numPattern( "person_hasInterest_tag" );
    public static final Pattern POST_HAS_TAG_TAG = numPattern( "post_hasTag_tag" );
    public static final Pattern PERSON_LIKES_POST = numPattern( "person_likes_post" );
    public static final Pattern FORUM_HAS_MEMBER_PERSON = numPattern( "forum_hasMember_person" );
    public static final Pattern FORUM_HAS_MEMBER_WITH_POSTS_PERSON = numPattern( "forum_hasMemberWithPosts_person" );
    public static final String FORUM_HAS_MEMBER_WITH_POSTS_PERSON_FILENAME = "forum_hasMemberWithPosts_person_0_0.csv";
    public static final Pattern FORUM_HAS_TAG_TAG = numPattern( "forum_hasTag_tag" );
    public static final Pattern PERSON_LIKES_COMMENT = numPattern( "person_likes_comment" );
    public static final Pattern COMMENT_HAS_TAG_TAG = numPattern( "comment_hasTag_tag" );

    private static Pattern numPattern( String prefix )
    {
        return Pattern.compile( format( "%s_%s_%s.csv", prefix, NUM_REGEX, NUM_REGEX ) );
    }
}
