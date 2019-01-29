/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.importer;

import java.util.regex.Pattern;

public class CsvFilesForMerge
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
    public static final Pattern PERSON_KNOWS_PERSON =
            Pattern.compile( "person_knows_person_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PERSON_STUDIES_AT_ORGANISATION =
            Pattern.compile( "person_studyAt_organisation_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PERSON_WORKS_AT_ORGANISATION =
            Pattern.compile( "person_workAt_organisation_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PERSON_HAS_INTEREST_TAG =
            Pattern.compile( "person_hasInterest_tag_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern POST_HAS_TAG_TAG =
            Pattern.compile( "post_hasTag_tag_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PERSON_LIKES_POST =
            Pattern.compile( "person_likes_post_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern FORUM_HAS_MEMBER_PERSON =
            Pattern.compile( "forum_hasMember_person_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern FORUM_HAS_MEMBER_WITH_POSTS_PERSON =
            Pattern.compile( "forum_hasMemberWithPosts_person_\\d{1,2}_\\d{1,2}.csv" );
    public static final String FORUM_HAS_MEMBER_WITH_POSTS_PERSON_FILENAME =
            "forum_hasMemberWithPosts_person_0_0.csv";
    public static final Pattern FORUM_HAS_TAG_TAG =
            Pattern.compile( "forum_hasTag_tag_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern PERSON_LIKES_COMMENT =
            Pattern.compile( "person_likes_comment_\\d{1,2}_\\d{1,2}.csv" );
    public static final Pattern COMMENT_HAS_TAG_TAG =
            Pattern.compile( "comment_hasTag_tag_\\d{1,2}_\\d{1,2}.csv" );

}
