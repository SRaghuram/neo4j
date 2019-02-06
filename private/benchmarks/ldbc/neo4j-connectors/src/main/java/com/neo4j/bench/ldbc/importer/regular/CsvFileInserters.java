/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer.regular;

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
import com.neo4j.bench.ldbc.connection.ImportDateUtil;
import com.neo4j.bench.ldbc.connection.LdbcDateCodecUtil;
import com.neo4j.bench.ldbc.importer.CsvFileInserter;
import com.neo4j.bench.ldbc.importer.CsvFiles;
import com.neo4j.bench.ldbc.importer.CsvLineInserter;
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
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.unsafe.batchinsert.BatchInserter;

import static com.neo4j.bench.ldbc.cli.LdbcCli.EMPTY_MAP;
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

    private final List<CsvFileInserter> commentsInserter;
    private final List<CsvFileInserter> commentHasTagTagInserter;
    private final List<CsvFileInserter> forumsInserter;
    private final List<CsvFileInserter> organizationsInserter;
    private final List<CsvFileInserter> personsInserter;
    private final List<CsvFileInserter> placesInserter;
    private final List<CsvFileInserter> postsInserter;
    private final List<CsvFileInserter> tagClassesInserter;
    private final List<CsvFileInserter> tagsInserter;
    private final List<CsvFileInserter> commentHasCreatorPersonInserter;
    private final List<CsvFileInserter> commentIsLocatedInPlaceInserter;
    private final List<CsvFileInserter> commentReplyOfCommentInserter;
    private final List<CsvFileInserter> commentReplyOfPostInserter;
    private final List<CsvFileInserter> forumContainerOfPostInserter;
    private final List<CsvFileInserter> forumHasMemberPersonInserter;
    private final List<CsvFileInserter> forumHasModeratorPersonInserter;
    private final List<CsvFileInserter> forumHasTagInserter;
    private final List<CsvFileInserter> personHasInterestTagInserter;
    private final List<CsvFileInserter> personIsLocatedInPlaceInserter;
    private final List<CsvFileInserter> personKnowsPersonInserter;
    private final List<CsvFileInserter> personLikesCommentInserter;
    private final List<CsvFileInserter> personLikesPostInserter;
    private final List<CsvFileInserter> personStudyAtOrganisationInserter;
    private final List<CsvFileInserter> personWorksAtOrganisationInserter;
    private final List<CsvFileInserter> placeIsPartOfPlaceInserter;
    private final List<CsvFileInserter> postHasCreatorPersonInserter;
    private final List<CsvFileInserter> postHasTagTagInserter;
    private final List<CsvFileInserter> postIsLocatedInPlaceInserter;
    private final List<CsvFileInserter> tagClassIsSubclassOfTagClassInserter;
    private final List<CsvFileInserter> tagHasTypeTagClassInserter;
    private final List<CsvFileInserter> organisationBasedNearPlaceInserter;

    CsvFileInserters(
            TempIndexFactory tempIndexFactory,
            final BatchInserter batchInserter,
            File csvDataDir,
            ImportDateUtil importDateUtil ) throws IOException
    {
        final Calendar calendar = LdbcDateCodecUtil.newCalendar();

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
        this.commentsInserter = comments(
                csvDataDir,
                batchInserter,
                commentsIndex,
                importDateUtil,
                calendar );
        this.forumsInserter = forums(
                csvDataDir,
                batchInserter,
                forumsIndex,
                importDateUtil,
                calendar );
        this.organizationsInserter = organizations(
                csvDataDir,
                batchInserter,
                organizationsIndex );
        this.personsInserter = persons(
                csvDataDir,
                batchInserter,
                personsIndex,
                importDateUtil,
                calendar );
        this.placesInserter = places(
                csvDataDir,
                batchInserter,
                placesIndex );
        this.postsInserter = posts(
                csvDataDir,
                batchInserter,
                postsIndex,
                importDateUtil,
                calendar );
        this.tagClassesInserter = tagClasses(
                csvDataDir,
                batchInserter,
                tagClassesIndex );
        this.tagsInserter = tags(
                csvDataDir,
                batchInserter,
                tagsIndex );

        /*
         * Relationship File Inserters
         */
        this.commentHasCreatorPersonInserter = commentHasCreatorPerson(
                csvDataDir,
                batchInserter,
                personsIndex,
                commentsIndex );
        this.commentIsLocatedInPlaceInserter = commentIsLocatedInPlace(
                csvDataDir,
                batchInserter,
                commentsIndex,
                placesIndex );
        this.commentReplyOfCommentInserter = commentReplyOfComment(
                csvDataDir,
                batchInserter,
                commentsIndex );
        this.commentReplyOfPostInserter = commentReplyOfPost(
                csvDataDir,
                batchInserter,
                commentsIndex,
                postsIndex );
        this.forumContainerOfPostInserter = forumContainerOfPost(
                csvDataDir,
                batchInserter,
                forumsIndex,
                postsIndex );
        this.forumHasMemberPersonInserter = forumHasMemberPerson(
                csvDataDir,
                batchInserter,
                forumsIndex,
                personsIndex,
                importDateUtil,
                calendar );
        this.forumHasModeratorPersonInserter = forumHasModeratorPerson(
                csvDataDir,
                batchInserter,
                personsIndex,
                forumsIndex );
        this.forumHasTagInserter = forumHasTag(
                csvDataDir,
                batchInserter,
                forumsIndex,
                tagsIndex );
        this.personHasInterestTagInserter = personHasInterestTag(
                csvDataDir,
                batchInserter,
                personsIndex,
                tagsIndex );
        this.personIsLocatedInPlaceInserter = personIsLocatedInPlace(
                csvDataDir,
                batchInserter,
                personsIndex,
                placesIndex );
        this.personKnowsPersonInserter = personKnowsPerson(
                csvDataDir,
                batchInserter,
                personsIndex,
                importDateUtil,
                calendar );
        this.personLikesCommentInserter = personLikesComment(
                csvDataDir,
                batchInserter,
                personsIndex,
                commentsIndex,
                importDateUtil,
                calendar );
        this.personLikesPostInserter = personLikesPost(
                csvDataDir,
                batchInserter,
                personsIndex,
                postsIndex,
                importDateUtil,
                calendar );
        this.personStudyAtOrganisationInserter = personStudyAtOrganisation(
                csvDataDir,
                batchInserter,
                personsIndex,
                organizationsIndex );
        this.personWorksAtOrganisationInserter = personWorksAtOrganisation(
                csvDataDir,
                batchInserter,
                personsIndex,
                organizationsIndex );
        this.placeIsPartOfPlaceInserter = placeIsPartOfPlace(
                csvDataDir,
                batchInserter,
                placesIndex );
        this.postHasCreatorPersonInserter = postHasCreatorPerson(
                csvDataDir,
                batchInserter,
                personsIndex,
                postsIndex );
        this.postHasTagTagInserter = postHasTagTag(
                csvDataDir,
                batchInserter,
                postsIndex,
                tagsIndex );
        this.commentHasTagTagInserter = commentHasTagTag(
                csvDataDir,
                batchInserter,
                commentsIndex,
                tagsIndex );
        this.postIsLocatedInPlaceInserter = postIsLocatedInPlace(
                csvDataDir,
                batchInserter,
                postsIndex,
                placesIndex );
        this.tagClassIsSubclassOfTagClassInserter = tagClassIsSubclassOfTagClass(
                csvDataDir,
                batchInserter,
                tagClassesIndex );
        this.tagHasTypeTagClassInserter = tagHasTypeTagClass(
                csvDataDir,
                batchInserter,
                tagsIndex,
                tagClassesIndex );
        this.organisationBasedNearPlaceInserter = organisationIsLocatedInPlace(
                csvDataDir,
                batchInserter,
                organizationsIndex,
                placesIndex );
    }

    List<CsvFileInserter> getCommentsInserters()
    {
        return commentsInserter;
    }

    List<CsvFileInserter> getForumsInserters()
    {
        return forumsInserter;
    }

    List<CsvFileInserter> getOrganizationsInserters()
    {
        return organizationsInserter;
    }

    List<CsvFileInserter> getPersonsInserters()
    {
        return personsInserter;
    }

    List<CsvFileInserter> getPlacesInserters()
    {
        return placesInserter;
    }

    List<CsvFileInserter> getPostsInserters()
    {
        return postsInserter;
    }

    List<CsvFileInserter> getTagClassesInserters()
    {
        return tagClassesInserter;
    }

    List<CsvFileInserter> getTagsInserters()
    {
        return tagsInserter;
    }

    List<CsvFileInserter> getCommentHasTagTagInserters()
    {
        return commentHasTagTagInserter;
    }

    List<CsvFileInserter> getCommentHasCreatorPersonInserters()
    {
        return commentHasCreatorPersonInserter;
    }

    List<CsvFileInserter> getCommentIsLocatedInPlaceInserters()
    {
        return commentIsLocatedInPlaceInserter;
    }

    List<CsvFileInserter> getCommentReplyOfCommentInserters()
    {
        return commentReplyOfCommentInserter;
    }

    List<CsvFileInserter> getCommentReplyOfPostInserters()
    {
        return commentReplyOfPostInserter;
    }

    List<CsvFileInserter> getForumContainerOfPostInserters()
    {
        return forumContainerOfPostInserter;
    }

    List<CsvFileInserter> getForumHasMemberPersonInserters()
    {
        return forumHasMemberPersonInserter;
    }

    List<CsvFileInserter> getForumHasModeratorPersonInserters()
    {
        return forumHasModeratorPersonInserter;
    }

    List<CsvFileInserter> getForumHasTagInserters()
    {
        return forumHasTagInserter;
    }

    List<CsvFileInserter> getPersonHasInterestTagInserters()
    {
        return personHasInterestTagInserter;
    }

    List<CsvFileInserter> getPersonIsLocatedInPlaceInserters()
    {
        return personIsLocatedInPlaceInserter;
    }

    List<CsvFileInserter> getPersonKnowsPersonInserters()
    {
        return personKnowsPersonInserter;
    }

    List<CsvFileInserter> getPersonLikesCommentInserters()
    {
        return personLikesCommentInserter;
    }

    List<CsvFileInserter> getPersonLikesPostInserters()
    {
        return personLikesPostInserter;
    }

    List<CsvFileInserter> getPersonStudyAtOrganisationInserters()
    {
        return personStudyAtOrganisationInserter;
    }

    List<CsvFileInserter> getPersonWorksAtOrganisationInserters()
    {
        return personWorksAtOrganisationInserter;
    }

    List<CsvFileInserter> getPlaceIsPartOfPlaceInserters()
    {
        return placeIsPartOfPlaceInserter;
    }

    List<CsvFileInserter> getPostHasCreatorPersonInserters()
    {
        return postHasCreatorPersonInserter;
    }

    List<CsvFileInserter> getPostHasTagTagInserters()
    {
        return postHasTagTagInserter;
    }

    List<CsvFileInserter> getPostIsLocatedInPlaceInserters()
    {
        return postIsLocatedInPlaceInserter;
    }

    List<CsvFileInserter> getTagClassIsSubclassOfTagClassInserters()
    {
        return tagClassIsSubclassOfTagClassInserter;
    }

    List<CsvFileInserter> getTagHasTypeTagClassInserters()
    {
        return tagHasTypeTagClassInserter;
    }

    List<CsvFileInserter> getOrganisationIsLocatedInPlaceInserters()
    {
        return organisationBasedNearPlaceInserter;
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
        id  creationDate            location IP     browserUsed     content                                 length
        00  2010-03-11T10:11:18Z    14.134.0.11     Chrome          About Michael Jordan, Association...    20
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.COMMENT.matcher( path.getFileName().toString() ).matches() )
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
                                                              importDateUtil.csvDateTimeToFormat( creationDateString,
                                                                                                  calendar );
                                                      properties.put( Message.CREATION_DATE, creationDate );
                                                  }
                                                  catch ( ParseException e )
                                                  {
                                                      throw new RuntimeException(
                                                              format( "Invalid Date string: %s", creationDateString ),
                                                              e );
                                                  }
                                                  properties.put( Message.LOCATION_IP, columnValues[2] );
                                                  properties.put( Message.BROWSER_USED, columnValues[3] );
                                                  if ( !((String) columnValues[4]).isEmpty() )
                                                  {
                                                      properties.put( Message.CONTENT, columnValues[4] );
                                                  }
                                                  int length = Integer.parseInt( (String) columnValues[5] );
                                                  properties.put( Message.LENGTH, length );
                                                  long commentNodeId = batchInserter.createNode(
                                                          properties,
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
        id      imageFile   creationDate            locationIP      browserUsed     language    content           length
        100     photo9.jpg  2010-03-11T05:28:04Z    27.99.128.8     Firefox         zh          About Michael
        Jordan... 20
        */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.POST.matcher( path.getFileName().toString() ).matches() )
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
        id      firstName   lastName    gender  birthday    creationDate            locationIP      browserUsed
        75      Fernanda    Alves       male    1984-12-15  2010-12-14T11:41:37Z    143.106.0.7     Firefox
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.PERSON.matcher( path.getFileName().toString() ).matches() )
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
                                                      importDateUtil.populateCalendarFromCsvDate( birthdayString, calendar );
                                                      properties.put( Person.BIRTHDAY,
                                                                      importDateUtil.calendarToFormat( calendar ) );
                                                      // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in
                                                      // range 1-12
                                                      properties.put( Person.BIRTHDAY_MONTH, calendar.get( Calendar.MONTH ) + 1 );
                                                      properties.put( Person.BIRTHDAY_DAY_OF_MONTH,
                                                                      calendar.get( Calendar.DAY_OF_MONTH ) );
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
                                                  properties.put( Person.LANGUAGES, ((String) columnValues[8]).split( ";" ) );
                                                  properties
                                                          .put( Person.EMAIL_ADDRESSES, ((String) columnValues[9]).split( ";" ) );
                                                  long personNodeId = batchInserter.createNode(
                                                          properties,
                                                          Nodes.Person );
                                                  personsIndex.put( id, personNodeId );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> forums(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final ForumsTempIndex forumIndex,
            final ImportDateUtil importDateUtil,
            final Calendar calendar ) throws IOException
    {
        /*
            id      title                       creationDate
            150     Wall of Fernanda Alves      2010-12-14T11:41:37Z
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.FORUM.matcher( path.getFileName().toString() ).matches() )
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
                    .filter( path -> CsvFiles.TAG.matcher( path.getFileName().toString() ).matches() )
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
                    .filter( path -> CsvFiles.TAGCLASS.matcher( path.getFileName().toString() ).matches() )
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
        id  type        name                        url
        00  university  Universidade_de_Pernambuco  http://dbpedia.org/resource/Universidade_de_Pernambuco
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.ORGANIZATION.matcher( path.getFileName().toString() ).matches() )
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
        id      name            url                                             type
        5170    South_America   http://dbpedia.org/resource/South_America       REGION
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.PLACE.matcher( path.getFileName().toString() ).matches() )
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

    private static List<CsvFileInserter> commentReplyOfComment(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final CommentsTempIndex commentsIndex ) throws IOException
    {
        /*
        Comment.id  Comment.id
        20          00
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.COMMENT_REPLY_OF_COMMENT.matcher( path.getFileName().toString() ).matches() )
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
                                                  long toComment =
                                                          commentsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromComment,
                                                          toComment,
                                                          Rels.REPLY_OF_COMMENT,
                                                          EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> commentReplyOfPost(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final CommentsTempIndex commentsIndex,
            final PostsTempIndex postsIndex ) throws IOException
    {
        /*
        Comment.id  Post.id
        00          100
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.COMMENT_REPLY_OF_POST.matcher( path.getFileName().toString() ).matches() )
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
                                                  long toPost = postsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromComment,
                                                          toPost,
                                                          Rels.REPLY_OF_POST,
                                                          EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> commentIsLocatedInPlace(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final CommentsTempIndex commentsIndex,
            final PlacesTempIndex placesIndex ) throws IOException
    {
        /*
        Comment.id  Place.id
        100         73
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.COMMENT_LOCATED_IN_PLACE.matcher( path.getFileName().toString() ).matches() )
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
                                                  long toPlace = placesIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromComment,
                                                          toPlace,
                                                          Rels.COMMENT_IS_LOCATED_IN,
                                                          EMPTY_MAP );
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
        Place.id Place.id
        11          5170
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.PLACE_IS_PART_OF_PLACE.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromPlace = placesIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toPlace = placesIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromPlace,
                                                          toPlace,
                                                          Rels.IS_PART_OF,
                                                          EMPTY_MAP );
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
                    .filter( path -> CsvFiles.PERSON_KNOWS_PERSON.matcher( path.getFileName().toString() ).matches() )
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
                    .filter( path -> CsvFiles.PERSON_STUDIES_AT_ORGANISATION.matcher( path.getFileName().toString() )
                                                                            .matches() )
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

    private static List<CsvFileInserter> commentHasCreatorPerson(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PersonsTempIndex personsIndex,
            final CommentsTempIndex commentsIndex ) throws IOException
    {
        /*
        Comment.id  Person.id
        00          1402
         */
        return Files.list( csvDataDir.toPath() )
                    .filter(
                            path -> CsvFiles.COMMENT_HAS_CREATOR_PERSON.matcher( path.getFileName().toString() ).matches() )
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
                                                  long toPerson = personsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromComment,
                                                          toPerson,
                                                          Rels.COMMENT_HAS_CREATOR,
                                                          EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> postHasCreatorPerson(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PersonsTempIndex personsIndex,
            final PostsTempIndex postsIndex ) throws IOException
    {
        /*
        Post.id     Person.id
        00          75
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.POST_HAS_CREATOR_PERSON.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromPost = postsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toPerson = personsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromPost,
                                                          toPerson,
                                                          Rels.POST_HAS_CREATOR,
                                                          EMPTY_MAP );
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
        Forum.id    Person.id
        1500        75
         */
        return Files.list( csvDataDir.toPath() )
                    .filter(
                            path -> CsvFiles.FORUM_HAS_MODERATOR_PERSON.matcher( path.getFileName().toString() ).matches() )
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
                                                  batchInserter.createRelationship(
                                                          fromForum,
                                                          toPerson,
                                                          Rels.HAS_MODERATOR,
                                                          EMPTY_MAP );
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
        Person.id   Place.id
        75          310
         */
        return Files.list( csvDataDir.toPath() )
                    .filter(
                            path -> CsvFiles.PERSON_IS_LOCATED_IN_PLACE.matcher( path.getFileName().toString() ).matches() )
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
                                                  long toPlace = placesIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromPerson,
                                                          toPlace,
                                                          Rels.PERSON_IS_LOCATED_IN,
                                                          EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> personWorksAtOrganisation(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PersonsTempIndex personsIndex,
            final OrganizationsTempIndex organizationsIndex ) throws IOException
    {
        /*
        Person.id   Organisation.id     workFrom
        75          10                  2016
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.PERSON_WORKS_AT_ORGANISATION.matcher( path.getFileName().toString() )
                                                                          .matches() )
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
                                                  Map<String,Object> properties = new HashMap<>();
                                                  properties.put( WorksAt.WORK_FROM, workFrom );
                                                  batchInserter.createRelationship(
                                                          fromPerson,
                                                          toOrganisation,
                                                          Rels.WORKS_AT,
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
                    .filter( path -> CsvFiles.PERSON_HAS_INTEREST_TAG.matcher( path.getFileName().toString() ).matches() )
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
                                                          EMPTY_MAP );
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
                    .filter( path -> CsvFiles.POST_HAS_TAG_TAG.matcher( path.getFileName().toString() ).matches() )
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
                                                          EMPTY_MAP );
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
                    .filter( path -> CsvFiles.COMMENT_HAS_TAG_TAG.matcher( path.getFileName().toString() ).matches() )
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
                                                          EMPTY_MAP );
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
                    .filter( path -> CsvFiles.PERSON_LIKES_COMMENT.matcher( path.getFileName().toString() ).matches() )
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
                    .filter( path -> CsvFiles.PERSON_LIKES_POST.matcher( path.getFileName().toString() ).matches() )
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

    private static List<CsvFileInserter> postIsLocatedInPlace(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final PostsTempIndex postsIndex,
            final PlacesTempIndex placesIndex ) throws IOException
    {
        /*
        Post.id     Place.id
        00          11
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.POST_IS_LOCATED_IN_PLACE.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromPost = postsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toPlace = placesIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromPost,
                                                          toPlace,
                                                          Rels.POST_IS_LOCATED_IN,
                                                          EMPTY_MAP );
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
            final Calendar calendar ) throws IOException
    {
        /*
        Forum.id    Person.id   joinDate
        150         1489        2011-01-02T01:01:10Z
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.FORUM_HAS_MEMBER_PERSON.matcher( path.getFileName().toString() ).matches() )
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
                                                  String joinDateString = (String) columnValues[2];
                                                  long joinDate;
                                                  try
                                                  {
                                                      joinDate = importDateUtil.csvDateTimeToFormat( joinDateString, calendar );
                                                  }
                                                  catch ( ParseException e )
                                                  {
                                                      throw new RuntimeException(
                                                              format( "Invalid Date string: %s", joinDateString ), e );
                                                  }
                                                  Map<String,Object> properties = new HashMap<>();
                                                  properties.put( HasMember.JOIN_DATE, joinDate );
                                                  batchInserter.createRelationship(
                                                          fromForum,
                                                          toPerson,
                                                          Rels.HAS_MEMBER,
                                                          properties );
                                              }
                                          } ) )
                    .collect( toList() );
    }

    private static List<CsvFileInserter> forumContainerOfPost(
            final File csvDataDir,
            final BatchInserter batchInserter,
            final ForumsTempIndex forumsIndex,
            final PostsTempIndex postsIndex ) throws IOException
    {
        /*
        Forum.id    Post.id
        40220       00
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.FORUMS_CONTAINER_OF_POST.matcher( path.getFileName().toString() ).matches() )
                    .map( path ->
                                  new CsvFileInserter(
                                          path.toFile(),
                                          new CsvLineInserter()
                                          {
                                              @Override
                                              public void insert( Object[] columnValues )
                                              {
                                                  long fromForum = forumsIndex.get( Long.parseLong( (String) columnValues[0] ) );
                                                  long toPost = postsIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromForum,
                                                          toPost,
                                                          Rels.CONTAINER_OF,
                                                          EMPTY_MAP );
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
                    .filter( path -> CsvFiles.FORUM_HAS_TAG_TAG.matcher( path.getFileName().toString() ).matches() )
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
                                                          EMPTY_MAP );
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
        Tag.id  TagClass.id
        259     211
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.TAG_HAS_TYPE_TAGCLASS.matcher( path.getFileName().toString() ).matches() )
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
                                                          tagClassesIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromTag,
                                                          toTagClass,
                                                          Rels.HAS_TYPE,
                                                          EMPTY_MAP );
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
                    .filter( path -> CsvFiles.TAGCLASS_IS_SUBCLASS_OF_TAGCLASS.matcher( path.getFileName().toString() )
                                                                              .matches() )
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
                                                  long toTagClass =
                                                          tagClassesIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromSubTagClass,
                                                          toTagClass,
                                                          Rels.IS_SUBCLASS_OF,
                                                          EMPTY_MAP );
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
        Organisation.id     Place.id
        00                  301
         */
        return Files.list( csvDataDir.toPath() )
                    .filter( path -> CsvFiles.ORGANISATION_IS_LOCATED_IN_PLACE.matcher( path.getFileName().toString() )
                                                                              .matches() )
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
                                                  long toPlace = placesIndex.get( Long.parseLong( (String) columnValues[1] ) );
                                                  batchInserter.createRelationship(
                                                          fromOrganisation,
                                                          toPlace,
                                                          Rels.ORGANISATION_IS_LOCATED_IN,
                                                          EMPTY_MAP );
                                              }
                                          } ) )
                    .collect( toList() );
    }
}
