/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer.dense1;

import com.neo4j.bench.ldbc.Domain.Forum;
import com.neo4j.bench.ldbc.Domain.HasMember;
import com.neo4j.bench.ldbc.Domain.Knows;
import com.neo4j.bench.ldbc.Domain.Likes;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Organisation;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Place;
import com.neo4j.bench.ldbc.Domain.Post;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.Domain.StudiesAt;
import com.neo4j.bench.ldbc.Domain.Tag;
import com.neo4j.bench.ldbc.Domain.TagClass;
import com.neo4j.bench.ldbc.Domain.WorksAt;
import com.neo4j.bench.ldbc.cli.LdbcCli;
import com.neo4j.bench.ldbc.connection.ImportDateUtil;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.TimeStampedRelationshipTypesCache;
import com.neo4j.bench.ldbc.importer.CsvFileInserter;
import com.neo4j.bench.ldbc.importer.CsvFilesForMerge;
import com.neo4j.bench.ldbc.importer.CsvLineInserter;
import com.neo4j.bench.ldbc.importer.ForumHasMemberWithPostsLoader;
import com.neo4j.bench.ldbc.importer.GraphMetadataTracker;
import com.neo4j.bench.ldbc.importer.tempindex.CommentsTempIndex;
import com.neo4j.bench.ldbc.importer.tempindex.ForumsTempIndex;
import com.neo4j.bench.ldbc.importer.tempindex.OrganizationsTempIndex;
import com.neo4j.bench.ldbc.importer.tempindex.PersonsTempIndex;
import com.neo4j.bench.ldbc.importer.tempindex.PlacesTempIndex;
import com.neo4j.bench.ldbc.importer.tempindex.PostsTempIndex;
import com.neo4j.bench.ldbc.importer.tempindex.TagClassesTempIndex;
import com.neo4j.bench.ldbc.importer.tempindex.TagsTempIndex;
import com.neo4j.bench.ldbc.importer.tempindex.TempIndexFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

class CsvFileInserters
{
    private final CommentsTempIndex commentsIndex;
    private final PostsTempIndex postsIndex;
    private final PersonsTempIndex personsIndex;
    private final ForumsTempIndex forumsIndex;
    private final TagsTempIndex tagsIndex;
    private final TagClassesTempIndex tagClassesIndex;
    private final OrganizationsTempIndex organizationsIndex;
    private final PlacesTempIndex placesIndex;

    private final List<CsvFileInserter> commentsInserters;
    private final List<CsvFileInserter> commentHasTagTagInserters;
    private final List<CsvFileInserter> forumsInserters;
    private final List<CsvFileInserter> organizationsInserters;
    private final List<CsvFileInserter> personsInserters;
    private final List<CsvFileInserter> placesInserters;
    private final List<CsvFileInserter> postsInserters;
    private final List<CsvFileInserter> tagClassesInserters;
    private final List<CsvFileInserter> tagsInserters;
    private final List<CsvFileInserter> comment_HasCreator_IsLocatedIn_ReplyOf_Inserters;
    private final List<CsvFileInserter> forumHasMemberPersonInserters;
    private final List<CsvFileInserter> forumHasMemberWithPostsPersonInserters;
    private final List<CsvFileInserter> forumHasModeratorPersonInserters;
    private final List<CsvFileInserter> forumHasTagInserters;
    private final List<CsvFileInserter> personHasInterestTagInserters;
    private final List<CsvFileInserter> personIsLocatedInPlaceInserters;
    private final List<CsvFileInserter> personKnowsPersonInserters;
    private final List<CsvFileInserter> personLikesCommentInserters;
    private final List<CsvFileInserter> personLikesPostInserters;
    private final List<CsvFileInserter> personStudyAtOrganisationInserters;
    private final List<CsvFileInserter> personWorksAtOrganisationInserters;
    private final List<CsvFileInserter> placeIsPartOfPlaceInserters;
    private final List<CsvFileInserter> post_HasCreator_HasContainer_InLocatedIn_Inserters;
    private final List<CsvFileInserter> postHasTagTagInserters;
    private final List<CsvFileInserter> tagClassIsSubclassOfTagClassInserters;
    private final List<CsvFileInserter> tagHasTypeTagClassInserters;
    private final List<CsvFileInserter> organisationBasedNearPlaceInserters;

    CsvFileInserters(
            TempIndexFactory tempIndexFactory,
            BatchInserter batchInserter,
            GraphMetadataTracker metadataTracker,
            File csvDataDir,
            ImportDateUtil importDateUtil ) throws IOException
    {
        final Calendar calendar = LdbcDateCodec.newCalendar();
        final TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache =
                new TimeStampedRelationshipTypesCache();

        ForumHasMemberWithPostsLoader.createIn( csvDataDir );

        /*
         * Temp Node ID Indexes
         */
        this.commentsIndex = new CommentsTempIndex( tempIndexFactory.create() );
        this.postsIndex = new PostsTempIndex( tempIndexFactory.create() );
        this.personsIndex = new PersonsTempIndex( tempIndexFactory.create() );
        this.forumsIndex = new ForumsTempIndex( tempIndexFactory.create() );
        this.tagsIndex = new TagsTempIndex( tempIndexFactory.create() );
        this.tagClassesIndex = new TagClassesTempIndex( tempIndexFactory.create() );
        this.organizationsIndex = new OrganizationsTempIndex( tempIndexFactory.create() );
        this.placesIndex = new PlacesTempIndex( tempIndexFactory.create() );

        /*
         * Node File Inserters
         */
        this.commentsInserters = comments(
                csvDataDir,
                batchInserter,
                commentsIndex,
                importDateUtil,
                calendar );
        this.forumsInserters = forums(
                csvDataDir,
                batchInserter,
                forumsIndex,
                importDateUtil,
                calendar );
        this.organizationsInserters = organizations(
                csvDataDir,
                batchInserter,
                organizationsIndex );
        this.personsInserters = persons(
                csvDataDir,
                batchInserter,
                personsIndex,
                importDateUtil,
                calendar );
        this.placesInserters = places(
                csvDataDir,
                batchInserter,
                placesIndex );
        this.postsInserters = posts(
                csvDataDir,
                batchInserter,
                postsIndex,
                importDateUtil,
                calendar );
        this.tagClassesInserters = tagClasses(
                csvDataDir,
                batchInserter,
                tagClassesIndex );
        this.tagsInserters = tags(
                csvDataDir,
                batchInserter,
                tagsIndex );

        /*
         * Relationship File Inserters
         */
        this.comment_HasCreator_IsLocatedIn_ReplyOf_Inserters = comment_HasCreator_IsLocatedIn_ReplyOf(
                csvDataDir,
                batchInserter,
                commentsIndex,
                postsIndex,
                personsIndex,
                placesIndex,
                importDateUtil,
                calendar,
                timeStampedRelationshipTypesCache,
                metadataTracker );
        this.post_HasCreator_HasContainer_InLocatedIn_Inserters = post_HasCreator_HasContainer_InLocatedIn(
                csvDataDir,
                batchInserter,
                personsIndex,
                postsIndex,
                forumsIndex,
                placesIndex,
                importDateUtil,
                calendar,
                timeStampedRelationshipTypesCache,
                metadataTracker );
        this.forumHasMemberPersonInserters = forumHasMemberPerson(
                csvDataDir,
                batchInserter,
                forumsIndex,
                personsIndex,
                importDateUtil,
                calendar,
                timeStampedRelationshipTypesCache,
                metadataTracker );
        this.forumHasMemberWithPostsPersonInserters = forumHasMemberWithPostsPerson(
                csvDataDir,
                batchInserter,
                forumsIndex,
                personsIndex,
                importDateUtil,
                calendar );
        this.forumHasModeratorPersonInserters = forumHasModeratorPerson(
                csvDataDir,
                batchInserter,
                personsIndex,
                forumsIndex );
        this.forumHasTagInserters = forumHasTag(
                csvDataDir,
                batchInserter,
                forumsIndex,
                tagsIndex );
        this.personHasInterestTagInserters = personHasInterestTag(
                csvDataDir,
                batchInserter,
                personsIndex,
                tagsIndex );
        this.personIsLocatedInPlaceInserters = personIsLocatedInPlace(
                csvDataDir,
                batchInserter,
                personsIndex,
                placesIndex );
        this.personKnowsPersonInserters = personKnowsPerson(
                csvDataDir,
                batchInserter,
                personsIndex,
                importDateUtil,
                calendar );
        this.personLikesCommentInserters = personLikesComment(
                csvDataDir,
                batchInserter,
                personsIndex,
                commentsIndex,
                importDateUtil,
                calendar );
        this.personLikesPostInserters = personLikesPost(
                csvDataDir,
                batchInserter,
                personsIndex,
                postsIndex,
                importDateUtil,
                calendar );
        this.personStudyAtOrganisationInserters = personStudyAtOrganisation(
                csvDataDir,
                batchInserter,
                personsIndex,
                organizationsIndex );
        this.personWorksAtOrganisationInserters = personWorksAtOrganisation(
                csvDataDir,
                batchInserter,
                personsIndex,
                organizationsIndex,
                metadataTracker,
                timeStampedRelationshipTypesCache );
        this.placeIsPartOfPlaceInserters = placeIsPartOfPlace(
                csvDataDir,
                batchInserter,
                placesIndex );
        this.postHasTagTagInserters = postHasTagTag(
                csvDataDir,
                batchInserter,
                postsIndex,
                tagsIndex );
        this.commentHasTagTagInserters = commentHasTagTag(
                csvDataDir,
                batchInserter,
                commentsIndex,
                tagsIndex );
        this.tagClassIsSubclassOfTagClassInserters = tagClassIsSubclassOfTagClass(
                csvDataDir,
                batchInserter,
                tagClassesIndex );
        this.tagHasTypeTagClassInserters = tagHasTypeTagClass(
                csvDataDir,
                batchInserter,
                tagsIndex,
                tagClassesIndex );
        this.organisationBasedNearPlaceInserters = organisationIsLocatedInPlace(
                csvDataDir,
                batchInserter,
                organizationsIndex,
                placesIndex );
    }

    List<CsvFileInserter> getCommentsInserters()
    {
        return commentsInserters;
    }

    List<CsvFileInserter> getForumsInserters()
    {
        return forumsInserters;
    }

    List<CsvFileInserter> getOrganizationsInserters()
    {
        return organizationsInserters;
    }

    List<CsvFileInserter> getPersonsInserters()
    {
        return personsInserters;
    }

    List<CsvFileInserter> getPlacesInserters()
    {
        return placesInserters;
    }

    List<CsvFileInserter> getPostsInserters()
    {
        return postsInserters;
    }

    List<CsvFileInserter> getTagClassesInserters()
    {
        return tagClassesInserters;
    }

    List<CsvFileInserter> getTagsInserters()
    {
        return tagsInserters;
    }

    List<CsvFileInserter> getCommentHasTagTagInserters()
    {
        return commentHasTagTagInserters;
    }

    List<CsvFileInserter> getComment_HasCreator_IsLocatedIn_ReplyOf_Inserters()
    {
        return comment_HasCreator_IsLocatedIn_ReplyOf_Inserters;
    }

    List<CsvFileInserter> getForumHasMemberPersonInserters()
    {
        return forumHasMemberPersonInserters;
    }

    List<CsvFileInserter> getForumHasMemberWithPostsPersonInserters()
    {
        return forumHasMemberWithPostsPersonInserters;
    }

    List<CsvFileInserter> getForumHasModeratorPersonInserters()
    {
        return forumHasModeratorPersonInserters;
    }

    List<CsvFileInserter> getForumHasTagInserters()
    {
        return forumHasTagInserters;
    }

    List<CsvFileInserter> getPersonHasInterestTagInserters()
    {
        return personHasInterestTagInserters;
    }

    List<CsvFileInserter> getPersonIsLocatedInPlaceInserters()
    {
        return personIsLocatedInPlaceInserters;
    }

    List<CsvFileInserter> getPersonKnowsPersonInserters()
    {
        return personKnowsPersonInserters;
    }

    List<CsvFileInserter> getPersonLikesCommentInserters()
    {
        return personLikesCommentInserters;
    }

    List<CsvFileInserter> getPersonLikesPostInserters()
    {
        return personLikesPostInserters;
    }

    List<CsvFileInserter> getPersonStudyAtOrganisationInserters()
    {
        return personStudyAtOrganisationInserters;
    }

    List<CsvFileInserter> getPersonWorksAtOrganisationInserters()
    {
        return personWorksAtOrganisationInserters;
    }

    List<CsvFileInserter> getPlaceIsPartOfPlaceInserters()
    {
        return placeIsPartOfPlaceInserters;
    }

    List<CsvFileInserter> getPost_HasCreator_HasContainer_InLocatedIn_Inserters()
    {
        return post_HasCreator_HasContainer_InLocatedIn_Inserters;
    }

    List<CsvFileInserter> getPostHasTagTagInserters()
    {
        return postHasTagTagInserters;
    }

    List<CsvFileInserter> getTagClassIsSubclassOfTagClassInserters()
    {
        return tagClassIsSubclassOfTagClassInserters;
    }

    List<CsvFileInserter> getTagHasTypeTagClassInserters()
    {
        return tagHasTypeTagClassInserters;
    }

    List<CsvFileInserter> getOrganisationIsLocatedInPlaceInserters()
    {
        return organisationBasedNearPlaceInserters;
    }

    void shutdownAll()
    {
        commentsIndex.shutdown();
        postsIndex.shutdown();
        personsIndex.shutdown();
        forumsIndex.shutdown();
        tagsIndex.shutdown();
        tagClassesIndex.shutdown();
        organizationsIndex.shutdown();
        placesIndex.shutdown();
    }

    private static List<CsvFileInserter> comments(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final CommentsTempIndex commentsIndex,
            final ImportDateUtil importDateUtil,
            final Calendar calendar ) throws IOException
    {
        /*
        id|             creationDate|                   locationIP|     browserUsed|    content|        length|
        creator|        place|  replyOfPost|    replyOfComment|
        25769803891|    2010-08-06T11:39:52.459+0000|   118.82.247.65|  Firefox|        About Augu..|   91|
        1099511628747|  29|         25769803877|              |
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.COMMENT.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  Map<String,Object> properties = new HashMap<>();
                                                  long id = Long.parseLong( (String) columnValues[0] );
                                                  properties.put( Message.ID, id );
                                                  String creationDateString = (String) columnValues[1];
                                                  try
                                                  {
                                                      // 2010-12-28T07:16:25Z
                                                      long creationDate =
                                                              importDateUtil.csvDateTimeToFormat( creationDateString, calendar );
                                                      properties.put( Message.CREATION_DATE, creationDate );
                                                  }
                                                  catch ( ParseException e )
                                                  {
                                                      throw new RuntimeException(
                                                              format( "Invalid Date string: %s", creationDateString ), e );
                                                  }
                                                  properties.put( Message.LOCATION_IP, columnValues[2] );
                                                  properties.put( Message.BROWSER_USED, columnValues[3] );
                                                  if ( !((String) columnValues[4]).isEmpty() )
                                                  {
                                                      properties.put( Message.CONTENT, columnValues[4] );
                                                  }
                                                  int length = Integer.parseInt( (String) columnValues[5] );
                                                  properties.put( Message.LENGTH, length );
                                                  long commentNodeId = batchInserter.createNode( properties );
                                                  batchInserter.setNodeLabels(
                                                          commentNodeId,
                                                          Nodes.Message,
                                                          Nodes.Comment );
                                                  commentsIndex.put( id, commentNodeId );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> posts(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PostsTempIndex postsIndex,
            final ImportDateUtil importDateUtil,
            final Calendar calendar ) throws IOException
    {
        /*
        id|         imageFile|      creationDate|                   locationIP|     browserUsed|    language|
        content|    length| creator|    Forum.id|       place|
        34359768|   photo3438.jpg|  2010-10-17T01:26:51.664+0000|   175.111.0.55|   Firefox|        |           |
               0|      68|         34359738370|    54|
        */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.POST.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  Map<String,Object> properties = new HashMap<>();
                                                  long id = Long.parseLong( (String) columnValues[0] );
                                                  properties.put( Message.ID, id );
                                                  if ( !((String) columnValues[1]).isEmpty() )
                                                  {
                                                      properties.put( Post.IMAGE_FILE, columnValues[1] );
                                                  }
                                                  String creationDateString = (String) columnValues[2];
                                                  try
                                                  {
                                                      // 2010-12-28T07:16:25Z
                                                      long creationDate =
                                                              importDateUtil.csvDateTimeToFormat( creationDateString, calendar );
                                                      properties.put( Message.CREATION_DATE, creationDate );
                                                  }
                                                  catch ( ParseException e )
                                                  {
                                                      throw new RuntimeException(
                                                              format( "Invalid Date string: %s", creationDateString ), e );
                                                  }
                                                  properties.put( Message.LOCATION_IP, columnValues[3] );
                                                  properties.put( Message.BROWSER_USED, columnValues[4] );
                                                  properties.put( Post.LANGUAGE, columnValues[5] );
                                                  if ( !((String) columnValues[6]).isEmpty() )
                                                  {
                                                      properties.put( Message.CONTENT, columnValues[6] );
                                                  }
                                                  properties.put( Message.LENGTH, Integer.parseInt( (String) columnValues[7] ) );
                                                  long postNodeId = batchInserter.createNode(
                                                          properties,
                                                          Nodes.Message,
                                                          Nodes.Post );
                                                  postsIndex.put( id, postNodeId );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> persons(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PersonsTempIndex personsIndex,
            final ImportDateUtil importDateUtil,
            final Calendar calendar ) throws IOException
    {
        /*
        0       1               2           3       4           5                               6               7
                  8
        id|     firstName|      lastName|   gender| birthday|   creationDate|                   locationIP|
        browserUsed|    place|
        68|     Mirza Kalich|   Ali|        male|   1989-11-02| 2010-03-27T13:28:38.717+0000|   175.111.0.55|
        Firefox|        794|
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.PERSON.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  Map<String,Object> properties = new HashMap<>();
                                                  long id = Long.parseLong( (String) columnValues[0] );
                                                  properties.put( Person.ID, id );
                                                  properties.put( Person.FIRST_NAME, columnValues[1] );
                                                  properties.put( Person.LAST_NAME, columnValues[2] );
                                                  properties.put( Person.GENDER, columnValues[3] );
                                                  String birthdayString = (String) columnValues[4];
                                                  try
                                                  {
                                                      properties.put(
                                                              Person.BIRTHDAY,
                                                              importDateUtil.csvDateToFormat( birthdayString, calendar ) );
                                                  }
                                                  catch ( ParseException e )
                                                  {
                                                      throw new RuntimeException(
                                                              format( "Invalid Date string: %s", birthdayString ), e );
                                                  }
                                                  String creationDateString = (String) columnValues[5];
                                                  try
                                                  {
                                                      long creationDate =
                                                              importDateUtil.csvDateTimeToFormat( creationDateString, calendar );
                                                      properties.put( Person.CREATION_DATE, creationDate );
                                                  }
                                                  catch ( ParseException e )
                                                  {
                                                      throw new RuntimeException(
                                                              format( "Invalid Date string: %s", creationDateString ), e );
                                                  }
                                                  properties.put( Person.LOCATION_IP, columnValues[6] );
                                                  properties.put( Person.BROWSER_USED, columnValues[7] );
                                                  // offset 8 is place
                                                  properties.put( Person.LANGUAGES, ((String) columnValues[9]).split( ";" ) );
                                                  properties.put( Person.EMAIL_ADDRESSES,
                                                                  ((String) columnValues[10]).split( ";" ) );
                                                  long personNodeId = batchInserter.createNode(
                                                          properties,
                                                          Nodes.Person );
                                                  personsIndex.put( id, personNodeId );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> forums( final File csvDataDir,
                                                 final BatchInserter batchInserter,
                                                 final ForumsTempIndex forumIndex,
                                                 final ImportDateUtil importDateUtil,
                                                 final Calendar calendar ) throws IOException
    {
        /*
            0       1               2                               3
            id|     title|          creationDate|                   moderator|
            0|      Wall of Mir..|  2010-03-27T13:28:48.717+0000|   68|
        */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.FORUM.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  Map<String,Object> properties = new HashMap<>();
                                                  long id = Long.parseLong( (String) columnValues[0] );
                                                  properties.put( Forum.ID, id );
                                                  properties.put( Forum.TITLE, columnValues[1] );
                                                  String creationDateString = (String) columnValues[2];
                                                  try
                                                  {
                                                      long creationDate =
                                                              importDateUtil.csvDateTimeToFormat( creationDateString, calendar );
                                                      properties.put( Forum.CREATION_DATE, creationDate );
                                                  }
                                                  catch ( ParseException e )
                                                  {
                                                      throw new RuntimeException(
                                                              format( "Invalid Date string: %s", creationDateString ), e );
                                                  }
                                                  long forumNodeId = batchInserter.createNode(
                                                          properties,
                                                          Nodes.Forum );
                                                  forumIndex.put( id, forumNodeId );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> tags(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final TagsTempIndex tagIndex ) throws IOException
    {
        /*
        id      name                url
        259     Gilberto_Gil        http://dbpedia.org/resource/Gilberto_Gil
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.TAG.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  Map<String,Object> properties = new HashMap<>();
                                                  long id = Long.parseLong( (String) columnValues[0] );
                                                  properties.put( Tag.ID, id );
                                                  properties.put( Tag.NAME, columnValues[1] );
                                                  long tagNodeId = batchInserter.createNode(
                                                          properties,
                                                          Nodes.Tag );
                                                  tagIndex.put( id, tagNodeId );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> tagClasses(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final TagClassesTempIndex tagClassesIndex ) throws IOException
    {
        /*
        id      name    url
        211     Person  http://dbpedia.org/ontology/Person
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.TAGCLASS.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  Map<String,Object> properties = new HashMap<>();
                                                  long id = Long.parseLong( (String) columnValues[0] );
                                                  properties.put( TagClass.NAME, columnValues[1] );
                                                  long tagClassNodeId = batchInserter.createNode(
                                                          properties,
                                                          Nodes.TagClass );
                                                  tagClassesIndex.put( id, tagClassNodeId );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> organizations(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final OrganizationsTempIndex organizationsIndex ) throws IOException
    {
        /*
        0   1           2           3                                       4
        id| type|       name|       url|                                    place|
        0|  company|    Kam_Air|    http://dbpedia.org/resource/Kam_Air|    59|
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.ORGANIZATION.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  Map<String,Object> properties = new HashMap<>();
                                                  long id = Long.parseLong( (String) columnValues[0] );
                                                  Label organizationTypeLabel =
                                                          stringToOrganisationType( (String) columnValues[1] );
                                                  properties.put( Organisation.ID, id );
                                                  properties.put( Organisation.NAME, columnValues[2] );
                                                  long organisationNodeId = batchInserter.createNode(
                                                          properties,
                                                          organizationTypeLabel );
                                                  organizationsIndex.put( id, organisationNodeId );
                                              }

                                              Organisation.Type stringToOrganisationType( String organisationTypeString )
                                              {
                                                  if ( organisationTypeString.toLowerCase().equals( "university" ) )
                                                  {
                                                      return Organisation.Type.University;
                                                  }
                                                  else if ( organisationTypeString.toLowerCase().equals( "company" ) )
                                                  {
                                                      return Organisation.Type.Company;
                                                  }
                                                  else
                                                  {
                                                      throw new RuntimeException(
                                                              "Unknown organisation type: " + organisationTypeString );
                                                  }
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> places(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PlacesTempIndex placeIndex ) throws IOException
    {
        /*
        0       1       2                           3           4
        id|     name|   url|                        type|       isPartOf|
        0|      India|  http://dbpedia.org/re..|    country|    1460|
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.PLACE.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  Map<String,Object> properties = new HashMap<>();
                                                  long id = Long.parseLong( (String) columnValues[0] );
                                                  properties.put( Place.ID, id );
                                                  properties.put( Place.NAME, columnValues[1] );
                                                  Place.Type placeType = stringToPlaceType( (String) columnValues[3] );
                                                  long placeNodeId = batchInserter.createNode(
                                                          properties,
                                                          placeType );
                                                  placeIndex.put( id, placeNodeId );
                                              }

                                              Place.Type stringToPlaceType( String placeTypeString )
                                              {
                                                  if ( placeTypeString.toLowerCase().equals( "city" ) )
                                                  {
                                                      return Place.Type.City;
                                                  }
                                                  if ( placeTypeString.toLowerCase().equals( "country" ) )
                                                  {
                                                      return Place.Type.Country;
                                                  }
                                                  if ( placeTypeString.toLowerCase().equals( "continent" ) )
                                                  {
                                                      return Place.Type.Continent;
                                                  }
                                                  throw new RuntimeException( "Unknown place type: " + placeTypeString );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> comment_HasCreator_IsLocatedIn_ReplyOf(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final CommentsTempIndex commentsIndex,
            final PostsTempIndex postsIndex,
            final PersonsTempIndex personsIndex,
            final PlacesTempIndex placesIndex,
            final ImportDateUtil importDateUtil,
            final Calendar calendar,
            final TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache,
            final GraphMetadataTracker metadataTracker ) throws IOException
    {
        /*
        0               1                               2               3               4               5       6
                  7           8               9
        id|             creationDate|                   locationIP|     browserUsed|    content|        length|
        creator|        place|      replyOfPost|    replyOfComment|
        25769803891|    2010-08-06T11:39:52.459+0000|   118.82.247.65|  Firefox|        About Augu..|   91|
        1099511628747|  29|         25769803877|    |
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.COMMENT.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromCommentNodeId =
                                                          commentsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toPersonNodeId =
                                                          personsIndex.get( Long.parseLong( (String) columnValues[6] ) );
                                                  long toPlaceNodeId =
                                                          placesIndex.get( Long.parseLong( (String) columnValues[7] ) );
                                                  long toMessageNodeId;
                                                  RelationshipType replyOfRelationshipType;
                                                  String replyOfPostString = (String) columnValues[8];
                                                  String replyOfCommentString = (String) columnValues[9];
                                                  if ( replyOfPostString.isEmpty() )
                                                  {
                                                      if ( replyOfCommentString.isEmpty() )
                                                      {
                                                          throw new RuntimeException(
                                                                  format( "Both replyOfPost and replyOfComment columns were " +
                                                                          "empty\n%s",
                                                                          Arrays.toString( columnValues ) ) );
                                                      }
                                                      else
                                                      {
                                                          // COMMENT REPLY OF COMMENT
                                                          toMessageNodeId =
                                                                  commentsIndex.get( Long.parseLong( replyOfCommentString ) );
                                                          replyOfRelationshipType = Rels.REPLY_OF_COMMENT;
                                                      }
                                                  }
                                                  else
                                                  {
                                                      if ( replyOfCommentString.isEmpty() )
                                                      {
                                                          // COMMENT REPLY OF POST
                                                          toMessageNodeId = postsIndex.get( Long.parseLong( replyOfPostString ) );
                                                          replyOfRelationshipType = Rels.REPLY_OF_POST;
                                                      }
                                                      else
                                                      {
                                                          throw new RuntimeException(
                                                                  format( "Both replyOfPost and replyOfComment columns had " +
                                                                          "values\n%s",
                                                                          Arrays.toString( columnValues ) ) );
                                                      }
                                                  }

                                                  long creationDateAtResolution;
                                                  try
                                                  {
                                                      creationDateAtResolution =
                                                              importDateUtil.queryDateUtil().formatToEncodedDateAtResolution(
                                                                      importDateUtil.csvDateTimeToFormat(
                                                                              (String) columnValues[1],
                                                                              calendar )
                                                                                                                            );
                                                  }
                                                  catch ( ParseException e )
                                                  {
                                                      throw new RuntimeException(
                                                              format( "Invalid Date string: %s", (String) columnValues[1] ), e );
                                                  }
                                                  metadataTracker
                                                          .recordCommentHasCreatorDateAtResolution( creationDateAtResolution );
                                                  RelationshipType hasCreatorRelationshipType =
                                                          timeStampedRelationshipTypesCache.commentHasCreatorForDateAtResolution(
                                                                  creationDateAtResolution,
                                                                  importDateUtil.queryDateUtil()
                                                                                                                                );
                                                  metadataTracker
                                                          .recordCommentIsLocatedInDateAtResolution( creationDateAtResolution );
                                                  RelationshipType isLocatedInRelationshipType =
                                                          timeStampedRelationshipTypesCache.commentIsLocatedInForDateAtResolution(
                                                                  creationDateAtResolution,
                                                                  importDateUtil.queryDateUtil()
                                                                                                                                 );
                                                  batchInserter.createRelationship(
                                                          fromCommentNodeId,
                                                          toPersonNodeId,
                                                          hasCreatorRelationshipType,
                                                          LdbcCli.EMPTY_MAP );
                                                  batchInserter.createRelationship(
                                                          fromCommentNodeId,
                                                          toPersonNodeId,
                                                          Rels.COMMENT_HAS_CREATOR,
                                                          LdbcCli.EMPTY_MAP );
                                                  batchInserter.createRelationship(
                                                          fromCommentNodeId,
                                                          toPlaceNodeId,
                                                          isLocatedInRelationshipType,
                                                          LdbcCli.EMPTY_MAP );
                                                  batchInserter.createRelationship(
                                                          fromCommentNodeId,
                                                          toMessageNodeId,
                                                          replyOfRelationshipType,
                                                          LdbcCli.EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> post_HasCreator_HasContainer_InLocatedIn(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PersonsTempIndex personsIndex,
            final PostsTempIndex postsIndex,
            final ForumsTempIndex forumsIndex,
            final PlacesTempIndex placesIndex,
            final ImportDateUtil importDateUtil,
            final Calendar calendar,
            final TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache,
            final GraphMetadataTracker metadataTracker ) throws IOException
    {
        /*
        0           1               2                               3               4               5           6
              7       8           9               10
        id|         imageFile|      creationDate|                   locationIP|     browserUsed|    language|
        content|    length| creator|    Forum.id|       place|
        34359768|   photo3438.jpg|  2010-10-17T01:26:51.664+0000|   175.111.0.55|   Firefox|        |           |
               0|      68|         34359738370|    54|
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.POST.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromPostNodeId =
                                                          postsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toPersonNodeId =
                                                          personsIndex.get( Long.parseLong( (String) columnValues[8] ) );
                                                  long toForumNodeId =
                                                          forumsIndex.get( Long.parseLong( (String) columnValues[9] ) );
                                                  long toPlaceNodeId =
                                                          placesIndex.get( Long.parseLong( (String) columnValues[10] ) );
                                                  long creationDateAtResolution;
                                                  try
                                                  {
                                                      creationDateAtResolution =
                                                              importDateUtil.queryDateUtil().formatToEncodedDateAtResolution(
                                                                      importDateUtil.csvDateTimeToFormat(
                                                                              (String) columnValues[2],
                                                                              calendar )
                                                                                                                            );
                                                  }
                                                  catch ( ParseException e )
                                                  {
                                                      throw new RuntimeException(
                                                              format( "Invalid Date string: %s", (String) columnValues[2] ), e );
                                                  }
                                                  metadataTracker
                                                          .recordPostHasCreatorDateAtResolution( creationDateAtResolution );
                                                  RelationshipType hasCreatorRelationshipType =
                                                          timeStampedRelationshipTypesCache.postHasCreatorForDateAtResolution(
                                                                  creationDateAtResolution,
                                                                  importDateUtil.queryDateUtil()
                                                                                                                             );
                                                  metadataTracker
                                                          .recordPostIsLocatedInDateAtResolution( creationDateAtResolution );
                                                  RelationshipType isLocatedInRelationshipType =
                                                          timeStampedRelationshipTypesCache.postIsLocatedInForDateAtResolution(
                                                                  creationDateAtResolution,
                                                                  importDateUtil.queryDateUtil()
                                                                                                                              );
                                                  batchInserter.createRelationship(
                                                          fromPostNodeId,
                                                          toPersonNodeId,
                                                          hasCreatorRelationshipType,
                                                          LdbcCli.EMPTY_MAP );
                                                  batchInserter.createRelationship(
                                                          fromPostNodeId,
                                                          toPersonNodeId,
                                                          Rels.POST_HAS_CREATOR,
                                                          LdbcCli.EMPTY_MAP );
                                                  batchInserter.createRelationship(
                                                          toForumNodeId,
                                                          fromPostNodeId,
                                                          Rels.CONTAINER_OF,
                                                          LdbcCli.EMPTY_MAP );
                                                  batchInserter.createRelationship(
                                                          fromPostNodeId,
                                                          toPlaceNodeId,
                                                          isLocatedInRelationshipType,
                                                          LdbcCli.EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> placeIsPartOfPlace(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PlacesTempIndex placesIndex ) throws IOException
    {
        /*
        0       1       2                           3           4
        id|     name|   url|                        type|       isPartOf|
        0|      India|  http://dbpedia.org/re..|    country|    1460|
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.PLACE.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  String fromPlaceString = (String) columnValues[0];
                                                  String toPlaceString = (String) columnValues[4];
                                                  if ( !toPlaceString.isEmpty() )
                                                  {
                                                      long fromPlace = placesIndex.get( Long.parseLong( fromPlaceString ) );
                                                      long toPlace = placesIndex.get( Long.parseLong( toPlaceString ) );
                                                      batchInserter.createRelationship(
                                                              fromPlace,
                                                              toPlace,
                                                              Rels.IS_PART_OF,
                                                              LdbcCli.EMPTY_MAP );
                                                  }
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> personKnowsPerson(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PersonsTempIndex personsIndex,
            final ImportDateUtil importDateUtil,
            final Calendar calendar ) throws IOException
    {
        /*
        Person.id   Person.id   creationDate
        75          1489        2011-01-20T11:18:41Z
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.PERSON_KNOWS_PERSON.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromPerson =
                                                          personsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toPerson = personsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  String creationDateString = (String) columnValues[2];
                                                  long creationDate;
                                                  try
                                                  {
                                                      creationDate =
                                                              importDateUtil.csvDateTimeToFormat( creationDateString, calendar );
                                                  }
                                                  catch ( ParseException e )
                                                  {
                                                      throw new RuntimeException(
                                                              format( "Invalid Date string: %s", creationDateString ), e );
                                                  }
                                                  Map<String,Object> properties = new HashMap<>();
                                                  properties.put( Knows.CREATION_DATE, creationDate );
                                                  batchInserter.createRelationship(
                                                          fromPerson,
                                                          toPerson,
                                                          Rels.KNOWS,
                                                          properties );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> personStudyAtOrganisation(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PersonsTempIndex personsIndex,
            final OrganizationsTempIndex organizationsIndex ) throws IOException
    {
        /*
        Person.id   Organisation.id classYear
        75          00                  2004
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.PERSON_STUDIES_AT_ORGANISATION.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromPerson =
                                                          personsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toOrganisation =
                                                          organizationsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  int classYear = Integer.parseInt( (String) columnValues[2] );
                                                  Map<String,Object> properties = new HashMap<>();
                                                  properties.put( StudiesAt.CLASS_YEAR, classYear );
                                                  batchInserter.createRelationship(
                                                          fromPerson,
                                                          toOrganisation,
                                                          Rels.STUDY_AT,
                                                          properties );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> forumHasModeratorPerson(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PersonsTempIndex personsIndex,
            final ForumsTempIndex forumsIndex ) throws IOException
    {
        /*
            0       1               2                               3
            id|     title|          creationDate|                   moderator|
            0|      Wall of Mir..|  2010-03-27T13:28:48.717+0000|   68|
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.FORUM.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromForum = forumsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toModeratorPerson =
                                                          personsIndex.get( Long.parseLong( (String) columnValues[3] ) );
                                                  batchInserter.createRelationship(
                                                          fromForum,
                                                          toModeratorPerson,
                                                          Rels.HAS_MODERATOR,
                                                          LdbcCli.EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> personIsLocatedInPlace(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PersonsTempIndex personsIndex,
            final PlacesTempIndex placesIndex ) throws IOException
    {
        /*
        0       1               2           3       4           5                               6               7
                  8
        id|     firstName|      lastName|   gender| birthday|   creationDate|                   locationIP|
        browserUsed|    place|
        68|     Mirza Kalich|   Ali|        male|   1989-11-02| 2010-03-27T13:28:38.717+0000|   175.111.0.55|
        Firefox|        794|
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.PERSON.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromPerson =
                                                          personsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toPlace = placesIndex.get( Long.parseLong( (String) columnValues[8] ) );
                                                  batchInserter.createRelationship(
                                                          fromPerson,
                                                          toPlace,
                                                          Rels.PERSON_IS_LOCATED_IN,
                                                          LdbcCli.EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> personWorksAtOrganisation(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PersonsTempIndex personsIndex,
            final OrganizationsTempIndex organizationsIndex,
            final GraphMetadataTracker graphMetadataTracker,
            final TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache ) throws IOException
    {
        /*
        Person.id   Organisation.id     workFrom
        75          10                  2016
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.PERSON_WORKS_AT_ORGANISATION.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromPerson =
                                                          personsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toOrganisation =
                                                          organizationsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  int workFrom = Integer.parseInt( (String) columnValues[2] );
                                                  graphMetadataTracker.recordWorkFromYear( workFrom );
                                                  Map<String,Object> properties = new HashMap<>();
                                                  properties.put( WorksAt.WORK_FROM, workFrom );
                                                  RelationshipType worksAtForYear =
                                                          timeStampedRelationshipTypesCache.worksAtForYear( workFrom );
                                                  batchInserter.createRelationship(
                                                          fromPerson,
                                                          toOrganisation,
                                                          worksAtForYear,
                                                          properties );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> personHasInterestTag(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PersonsTempIndex personsIndex,
            final TagsTempIndex tagsIndex ) throws IOException
    {
        /*
        Person.id   Tag.id
        75          259
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.PERSON_HAS_INTEREST_TAG.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromPerson =
                                                          personsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toTag = tagsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromPerson,
                                                          toTag,
                                                          Rels.HAS_INTEREST,
                                                          LdbcCli.EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> postHasTagTag(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PostsTempIndex postsIndex,
            final TagsTempIndex tagsIndex ) throws IOException
    {
        /*
        Post.id Tag.id
        100     2903
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.POST_HAS_TAG_TAG.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromPost = postsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toTag = tagsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromPost,
                                                          toTag,
                                                          Rels.POST_HAS_TAG,
                                                          LdbcCli.EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> commentHasTagTag(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final CommentsTempIndex commentsIndex,
            final TagsTempIndex tagsIndex ) throws IOException
    {
        /*
        Comment.id Tag.id
        100     2903
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.COMMENT_HAS_TAG_TAG.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromComment =
                                                          commentsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toTag = tagsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromComment,
                                                          toTag,
                                                          Rels.COMMENT_HAS_TAG,
                                                          LdbcCli.EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> personLikesComment(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PersonsTempIndex personsIndex,
            final CommentsTempIndex commentsIndex,
            final ImportDateUtil importDateUtil,
            final Calendar calendar ) throws IOException
    {
        /*
        Person.id   Post.id     creationDate
        1489        00          2011-01-20T11:18:41Z
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.PERSON_LIKES_COMMENT.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromPerson =
                                                          personsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toComment =
                                                          commentsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  long creationDate;
                                                  try
                                                  {
                                                      creationDate = importDateUtil
                                                              .csvDateTimeToFormat( (String) columnValues[2], calendar );
                                                  }
                                                  catch ( ParseException e )
                                                  {
                                                      throw new RuntimeException(
                                                              format( "Invalid Date string: %s", columnValues[2] ), e );
                                                  }
                                                  Map<String,Object> properties = new HashMap<>();
                                                  properties.put( Likes.CREATION_DATE, creationDate );
                                                  batchInserter.createRelationship(
                                                          fromPerson,
                                                          toComment,
                                                          Rels.LIKES_COMMENT,
                                                          properties );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> personLikesPost(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PersonsTempIndex personsIndex,
            final PostsTempIndex postsIndex,
            final ImportDateUtil importDateUtil,
            final Calendar calendar ) throws IOException
    {
        /*
        Person.id   Post.id     creationDate
        1489        00          2011-01-20T11:18:41Z
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.PERSON_LIKES_POST.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromPerson =
                                                          personsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toPost = postsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  long creationDate;
                                                  try
                                                  {
                                                      creationDate = importDateUtil
                                                              .csvDateTimeToFormat( (String) columnValues[2], calendar );
                                                  }
                                                  catch ( ParseException e )
                                                  {
                                                      throw new RuntimeException(
                                                              format( "Invalid Date string: %s", columnValues[2] ), e );
                                                  }
                                                  Map<String,Object> properties = new HashMap<>();
                                                  properties.put( Likes.CREATION_DATE, creationDate );
                                                  batchInserter.createRelationship(
                                                          fromPerson,
                                                          toPost,
                                                          Rels.LIKES_POST,
                                                          properties );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> forumHasMemberPerson(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final ForumsTempIndex forumsIndex,
            final PersonsTempIndex personsIndex,
            final ImportDateUtil importDateUtil,
            final Calendar calendar,
            final TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache,
            final GraphMetadataTracker metadataTracker ) throws IOException
    {
        /*
        Forum.id    Person.id   joinDate
        150         1489        2011-01-02T01:01:10Z
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.FORUM_HAS_MEMBER_PERSON.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromForum = forumsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toPerson = personsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  long joinDate;
                                                  try
                                                  {
                                                      joinDate = importDateUtil
                                                              .csvDateTimeToFormat( (String) columnValues[2], calendar );
                                                  }
                                                  catch ( ParseException e )
                                                  {
                                                      throw new RuntimeException(
                                                              format( "Invalid Date string: %s", columnValues[2] ), e );
                                                  }
                                                  long joinDateAtResolution =
                                                          importDateUtil.queryDateUtil()
                                                                        .formatToEncodedDateAtResolution( joinDate );
                                                  metadataTracker.recordHasMemberDateAtResolution( joinDateAtResolution );
                                                  RelationshipType hasMemberRelationshipType =
                                                          timeStampedRelationshipTypesCache.hasMemberForDateAtResolution(
                                                                  joinDateAtResolution,
                                                                  importDateUtil.queryDateUtil()
                                                                                                                        );
                                                  Map<String,Object> properties = new HashMap<>();
                                                  properties.put( HasMember.JOIN_DATE, joinDate );
                                                  batchInserter.createRelationship(
                                                          fromForum,
                                                          toPerson,
                                                          hasMemberRelationshipType,
                                                          properties );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> forumHasMemberWithPostsPerson(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final ForumsTempIndex forumsIndex,
            final PersonsTempIndex personsIndex,
            final ImportDateUtil importDateUtil,
            final Calendar calendar ) throws IOException
    {
        /*
        Forum.id    Person.id   joinDate
        150         1489        8273987908302
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.FORUM_HAS_MEMBER_WITH_POSTS_PERSON.asPredicate()
                                                                                        .test( path.toString() ) )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromForum = forumsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toPerson = personsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  long joinDate;
                                                  try
                                                  {
                                                      joinDate = importDateUtil
                                                              .csvDateTimeToFormat( (String) columnValues[2], calendar );
                                                  }
                                                  catch ( ParseException e )
                                                  {
                                                      throw new RuntimeException(
                                                              format( "Invalid Date string: %s", columnValues[2] ), e );
                                                  }
                                                  Map<String,Object> properties = new HashMap<>();
                                                  properties.put( HasMember.JOIN_DATE, joinDate );
                                                  batchInserter.createRelationship(
                                                          fromForum,
                                                          toPerson,
                                                          Rels.HAS_MEMBER_WITH_POSTS,
                                                          properties );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> forumHasTag(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final ForumsTempIndex forumsIndex,
            final TagsTempIndex tagsIndex ) throws IOException
    {
        /*
        Forum.id    Tag.id
        75          259
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.FORUM_HAS_TAG_TAG.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromForum = forumsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toTag = tagsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromForum,
                                                          toTag,
                                                          Rels.FORUM_HAS_TAG,
                                                          LdbcCli.EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> tagHasTypeTagClass(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final TagsTempIndex tagsIndex,
            final TagClassesTempIndex tagClassesIndex ) throws IOException
    {
        /*
        id|name|url|hasType
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.TAG.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromTag = tagsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toTagClass =
                                                          tagClassesIndex.get( Long.parseLong( (String) columnValues[3] ) );
                                                  batchInserter.createRelationship(
                                                          fromTag,
                                                          toTagClass,
                                                          Rels.HAS_TYPE,
                                                          LdbcCli.EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> tagClassIsSubclassOfTagClass(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final TagClassesTempIndex tagClassesIndex ) throws IOException
    {
        /*
        TagClass.id     TagClass.id
        211             239
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.TAGCLASS.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromSubTagClass =
                                                          tagClassesIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  String toTagClassIdString = (String) columnValues[3];
                                                  if ( !toTagClassIdString.isEmpty() )
                                                  {
                                                      long toTagClass =
                                                              tagClassesIndex.get( Long.parseLong( toTagClassIdString ) );
                                                      batchInserter.createRelationship(
                                                              fromSubTagClass,
                                                              toTagClass,
                                                              Rels.IS_SUBCLASS_OF,
                                                              LdbcCli.EMPTY_MAP );
                                                  }
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> organisationIsLocatedInPlace(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final OrganizationsTempIndex organizationsIndex,
            final PlacesTempIndex placesIndex ) throws IOException
    {
        /*
        0   1           2           3                                       4
        id| type|       name|       url|                                    place|
        0|  company|    Kam_Air|    http://dbpedia.org/resource/Kam_Air|    59|
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFilesForMerge.ORGANIZATION.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromOrganisation =
                                                          organizationsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toPlace = placesIndex.get( Long.parseLong( (String) columnValues[4] ) );
                                                  batchInserter.createRelationship(
                                                          fromOrganisation,
                                                          toPlace,
                                                          Rels.ORGANISATION_IS_LOCATED_IN,
                                                          LdbcCli.EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }
}
