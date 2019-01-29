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

package com.neo4j.bench.ldbc.importer.regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.util.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.cli.LdbcCli;
import com.neo4j.bench.ldbc.connection.GraphMetadataProxy;
import com.neo4j.bench.ldbc.connection.ImportDateUtil;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.AdditiveLabelFromColumnDecorator;
import com.neo4j.bench.ldbc.importer.CsvFiles;
import com.neo4j.bench.ldbc.importer.DateTimeDecorator;
import com.neo4j.bench.ldbc.importer.GraphMetadataTracker;
import com.neo4j.bench.ldbc.importer.LdbcHeaderFactory;
import com.neo4j.bench.ldbc.importer.LdbcImporterConfig;
import com.neo4j.bench.ldbc.importer.LdbcIndexer;
import com.neo4j.bench.ldbc.importer.LdbcSnbImporter;
import com.neo4j.bench.ldbc.importer.PersonDecorator;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.input.Collectors;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactory;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.input.csv.Type;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static java.lang.String.format;

public class LdbcSnbImporterParallelRegular extends LdbcSnbImporter
{
    private static final Logger LOGGER = Logger.getLogger( LdbcSnbImporterParallelRegular.class );

    private static class IndexSpace
    {
        static final String MESSAGES = "messages_id_space";
        static final String FORUMS = "forums_id_space";
        static final String ORGANIZATIONS = "organizations_id_space";
        static final String PERSONS = "persons_id_space";
        static final String PLACES = "places_id_space";
        static final String TAG_CLASSES = "tag_classes_id_space";
        static final String TAGS = "tags_id_space";
        static final String _ = "id_spaces_only_used_for_identifiers";
    }

    @Override
    public void load(
            File dbDir,
            File csvDataDir,
            File importerProperties,
            LdbcDateCodec.Format fromCsvFormat,
            LdbcDateCodec.Format toNeo4JFormat,
            LdbcDateCodec.Resolution timestampResolution,
            boolean withUnique,
            boolean withMandatory ) throws IOException, DbException
    {
        LOGGER.info( format( "Source CSV Dir:        %s", csvDataDir ) );
        LOGGER.info( format( "Target DB Dir:         %s", dbDir ) );
        LOGGER.info( format( "Source Date Format:    %s", fromCsvFormat.name() ) );
        LOGGER.info( format( "Target Date Format:    %s", toNeo4JFormat.name() ) );
        LOGGER.info( format( "Timestamp Resolution:  %s", timestampResolution.name() ) );
        LOGGER.info( format( "With Unique:           %s", withUnique ) );
        LOGGER.info( format( "With Mandatory:        %s", withMandatory ) );

        LOGGER.info( format( "Clear DB directory: %s", dbDir ) );
        FileUtils.deleteDirectory( dbDir );
        GraphMetadataTracker metadataTracker = new GraphMetadataTracker(
                toNeo4JFormat,
                timestampResolution,
                Neo4jSchema.NEO4J_REGULAR
        );

        LdbcIndexer indexer = new LdbcIndexer(
                metadataTracker.neo4jSchema(),
                withUnique,
                withMandatory );

        Extractors extractors = new Extractors( ';' );

        org.neo4j.unsafe.impl.batchimport.input.csv.Configuration configuration =
                new org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.Default()
                {
                    @Override
                    public char delimiter()
                    {
                        return '|';
                    }

                    @Override
                    public char arrayDelimiter()
                    {
                        return ';';
                    }
                };

        /*
        *** NODE FILES ***
         */
        Stream<Path> commentsFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.COMMENT.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> forumsFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.FORUM.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> organizationsFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.ORGANIZATION.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> personsFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> placesFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PLACE.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> postsFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.POST.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> tagClassesFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.TAGCLASS.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> tagsFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.TAG.matcher( path.getFileName().toString() ).matches() );

        /*
        *** RELATIONSHIP FILES ***
         */
        Stream<Path> commentHasCreatorPersonFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.COMMENT_HAS_CREATOR_PERSON.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> commentIsLocatedInPlaceFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.COMMENT_LOCATED_IN_PLACE.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> commentReplyOfCommentFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.COMMENT_REPLY_OF_COMMENT.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> commentReplyOfPostFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.COMMENT_REPLY_OF_POST.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> forumContainerOfPostFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.FORUMS_CONTAINER_OF_POST.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> forumHasMemberPersonFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.FORUM_HAS_MEMBER_PERSON.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> forumHasModeratorPersonFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.FORUM_HAS_MODERATOR_PERSON.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> forumHasTagFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.FORUM_HAS_TAG_TAG.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> personHasInterestTagFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON_HAS_INTEREST_TAG.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> personIsLocatedInPlaceFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON_IS_LOCATED_IN_PLACE.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> personKnowsPersonFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON_KNOWS_PERSON.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> personLikesCommentFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON_LIKES_COMMENT.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> personLikesPostFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON_LIKES_POST.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> personStudyAtOrganisationFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON_STUDIES_AT_ORGANISATION.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> personWorksAtOrganisationFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON_WORKS_AT_ORGANISATION.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> placeIsPartOfPlaceFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PLACE_IS_PART_OF_PLACE.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> postHasCreatorPersonFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.POST_HAS_CREATOR_PERSON.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> postHasTagTagFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.POST_HAS_TAG_TAG.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> commentHasTagTagFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.COMMENT_HAS_TAG_TAG.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> postIsLocatedInPlaceFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.POST_IS_LOCATED_IN_PLACE.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> tagClassIsSubclassOfTagClassFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.TAGCLASS_IS_SUBCLASS_OF_TAGCLASS.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> tagHasTypeTagClassFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.TAG_HAS_TYPE_TAGCLASS.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> organisationIsLocatedInPlaceFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.ORGANISATION_IS_LOCATED_IN_PLACE.matcher( path.getFileName().toString() ).matches() );

        /*
        *** NODE FILES ***
         */
        List<DataFactory<InputNode>> nodeDataFactories = new ArrayList<>();
        List<Header> nodeHeaders = new ArrayList<>();

        // comments: id|creationDate|locationIP|browserUsed|content|length|
        commentsFiles.forEach( path ->
                {
                    nodeDataFactories.add( DataFactories.data(
                            InputEntityDecorators.decorators(
                                    new DateTimeDecorator<>(
                                            Message.CREATION_DATE,
                                            ImportDateUtil.createFor( fromCsvFormat, toNeo4JFormat,
                                                    timestampResolution ) ),
                                    InputEntityDecorators.additiveLabels( new String[]{
                                            Nodes.Comment.name(),
                                            Nodes.Message.name()} )
                            ),
                            LdbcCli.CHARSET,
                            path.toFile()
                    ) );
                    nodeHeaders.add( new Header(
                            new Header.Entry( Message.ID, Type.ID, IndexSpace.MESSAGES, extractors.long_() ),
                            new Header.Entry( Message.CREATION_DATE, Type.PROPERTY, IndexSpace._, extractors.string() ),
                            new Header.Entry( Message.LOCATION_IP, Type.PROPERTY, IndexSpace._, extractors.string() ),
                            new Header.Entry( Message.BROWSER_USED, Type.PROPERTY, IndexSpace._, extractors.string() ),
                            new Header.Entry( Message.CONTENT, Type.PROPERTY, IndexSpace._, extractors.string() ),
                            new Header.Entry( Message.LENGTH, Type.PROPERTY, IndexSpace._, extractors.int_() ) ) );
                }
        );

        // posts: id|imageFile|creationDate|locationIP|browserUsed|language|content|length|
        postsFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new DateTimeDecorator<>(
                                    Message.CREATION_DATE,
                                    ImportDateUtil.createFor( fromCsvFormat, toNeo4JFormat,
                                            timestampResolution ) ),
                            InputEntityDecorators.additiveLabels( new String[]{
                                    Nodes.Post.name(),
                                    Nodes.Message.name()} )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Message.ID, Type.ID, IndexSpace.MESSAGES, extractors.long_() ),
                    new Header.Entry( Post.IMAGE_FILE, Type.PROPERTY, IndexSpace._, extractors.string() ),
                    new Header.Entry( Message.CREATION_DATE, Type.PROPERTY, IndexSpace._, extractors.string() ),
                    new Header.Entry( Message.LOCATION_IP, Type.PROPERTY, IndexSpace._, extractors.string() ),
                    new Header.Entry( Message.BROWSER_USED, Type.PROPERTY, IndexSpace._, extractors.string() ),
                    new Header.Entry( Post.LANGUAGE, Type.PROPERTY, IndexSpace._, extractors.string() ),
                    new Header.Entry( Message.CONTENT, Type.PROPERTY, IndexSpace._, extractors.string() ),
                    new Header.Entry( Message.LENGTH, Type.PROPERTY, IndexSpace._, extractors.int_() ) ) );
        } );

        // forums: id|title|creationDate|
        forumsFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new DateTimeDecorator<>(
                                    Forum.CREATION_DATE,
                                    ImportDateUtil.createFor( fromCsvFormat, toNeo4JFormat,
                                            timestampResolution ) ),
                            InputEntityDecorators.additiveLabels( new String[]{
                                    Nodes.Forum.name()} )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Forum.ID, Type.ID, IndexSpace.FORUMS, extractors.long_() ),
                    new Header.Entry( Forum.TITLE, Type.PROPERTY, IndexSpace._, extractors.string() ),
                    new Header.Entry( Forum.CREATION_DATE, Type.PROPERTY, IndexSpace._, extractors.string() ) ) );
        } );

        // organizations: id|type|name|url|
        organizationsFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            ( InputNode i ) -> i, // identify
                            new AdditiveLabelFromColumnDecorator( 1 )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Organisation.ID, Type.ID, IndexSpace.ORGANIZATIONS, extractors.long_() ),
                    // TODO need to make "type" available to label decorator but not actually import that property
                    new Header.Entry( "type", Type.PROPERTY, IndexSpace._, extractors.string() ),
                    new Header.Entry( Organisation.NAME, Type.PROPERTY, IndexSpace._, extractors.string() ),
                    new Header.Entry( null, Type.IGNORE, IndexSpace._, extractors.string() ) ) );
        } );

        // persons: id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed|
        personsFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new PersonDecorator(
                                    ImportDateUtil.createFor( fromCsvFormat, toNeo4JFormat,
                                            timestampResolution )
                            ),
                            InputEntityDecorators.additiveLabels( new String[]{
                                    Nodes.Person.name()} )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            nodeHeaders.add(
                    new Header(
                            new Header.Entry( Person.ID, Type.ID, IndexSpace.PERSONS, extractors.long_() ),
                            new Header.Entry( Person.FIRST_NAME, Type.PROPERTY, IndexSpace._, extractors.string() ),
                            new Header.Entry( Person.LAST_NAME, Type.PROPERTY, IndexSpace._, extractors.string() ),
                            new Header.Entry( Person.GENDER, Type.PROPERTY, IndexSpace._, extractors.string() ),
                            new Header.Entry( Person.BIRTHDAY, Type.PROPERTY, IndexSpace._, extractors.string() ),
                            new Header.Entry( Person.CREATION_DATE, Type.PROPERTY, IndexSpace._, extractors.string() ),
                            new Header.Entry( Person.LOCATION_IP, Type.PROPERTY, IndexSpace._, extractors.string() ),
                            new Header.Entry( Person.BROWSER_USED, Type.PROPERTY, IndexSpace._, extractors.string() ),
                            new Header.Entry( Person.LANGUAGES, Type.PROPERTY, IndexSpace._, extractors.stringArray() ),
                            new Header.Entry( Person.EMAIL_ADDRESSES, Type.PROPERTY, IndexSpace._, extractors
                                    .stringArray() ) ) );
        } );

        // places: id|name|url|type|
        placesFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            ( InputNode i ) -> i, // identify
                            new AdditiveLabelFromColumnDecorator( 2 )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Place.ID, Type.ID, IndexSpace.PLACES, extractors.long_() ),
                    new Header.Entry( Place.NAME, Type.PROPERTY, IndexSpace._, extractors.string() ),
                    new Header.Entry( null, Type.IGNORE, IndexSpace._, extractors.string() ),
                    // TODO need to make "type" available to label decorator but not actually import that property
                    new Header.Entry( "type", Type.PROPERTY, IndexSpace._, extractors.string() ) ) );
        } );

        // tag classes: id|name|url|
        tagClassesFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    InputEntityDecorators.additiveLabels( new String[]{
                            Nodes.TagClass.name()} ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( null, Type.ID, IndexSpace.TAG_CLASSES, extractors.long_() ),
                    new Header.Entry( TagClass.NAME, Type.PROPERTY, IndexSpace._, extractors.string() ),
                    new Header.Entry( null, Type.IGNORE, IndexSpace._, extractors.string() ) ) );
        } );

        // tags: id|name|url|
        tagsFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    InputEntityDecorators.additiveLabels( new String[]{
                            Nodes.Tag.name()} ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Tag.ID, Type.ID, IndexSpace.TAGS, extractors.long_() ),
                    new Header.Entry( Tag.NAME, Type.PROPERTY, IndexSpace._, extractors.string() ),
                    new Header.Entry( null, Type.IGNORE, IndexSpace._, extractors.string() ) ) );
        } );

        /*
        *** RELATIONSHIP FILES ***
         */
        List<DataFactory<InputRelationship>> relationshipDataFactories = new ArrayList<>();
        List<Header> relationshipHeaders = new ArrayList<>();

        // comment has creator person: Comment.id|Person.id|
        commentHasCreatorPersonFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.COMMENT_HAS_CREATOR.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.MESSAGES, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.PERSONS, extractors.long_() ) ) );
        } );

        // comment is located in place: Comment.id|Place.id|
        commentIsLocatedInPlaceFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.COMMENT_IS_LOCATED_IN.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.MESSAGES, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.PLACES, extractors.long_() ) ) );
        } );

        // comment reply of comment: Comment.id|Comment.id|
        commentReplyOfCommentFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.REPLY_OF_COMMENT.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.MESSAGES, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.MESSAGES, extractors.long_() ) ) );
        } );

        // comment reply of post: Comment.id|Post.id|
        commentReplyOfPostFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.REPLY_OF_POST.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.MESSAGES, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.MESSAGES, extractors.long_() ) ) );
        } );

        // forum container of post: Forum.id|Post.id|
        forumContainerOfPostFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.CONTAINER_OF.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.FORUMS, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.MESSAGES, extractors.long_() ) ) );
        } );

        // forum has member person: Forum.id|Person.id|joinDate|
        forumHasMemberPersonFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new DateTimeDecorator<>(
                                    HasMember.JOIN_DATE,
                                    ImportDateUtil.createFor( fromCsvFormat, toNeo4JFormat,
                                            timestampResolution ) ),
                            InputEntityDecorators.defaultRelationshipType(
                                    Rels.HAS_MEMBER.name() )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.FORUMS, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.PERSONS, extractors.long_() ),
                    new Header.Entry( HasMember.JOIN_DATE, Type.PROPERTY, IndexSpace._, extractors.string() ) ) );
        } );

        // forum has moderator person: Forum.id|Person.id|
        forumHasModeratorPersonFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.HAS_MODERATOR.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.FORUMS, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.PERSONS, extractors.long_() ) ) );
        } );

        // forum has tag: Forum.id|Tag.id|
        forumHasTagFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.FORUM_HAS_TAG.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.FORUMS, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.TAGS, extractors.long_() ) ) );
        } );

        // person has interest tag: Person.id|Tag.id|
        personHasInterestTagFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.HAS_INTEREST.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.PERSONS, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.TAGS, extractors.long_() ) ) );
        } );

        // person is located in place: Person.id|Place.id|
        personIsLocatedInPlaceFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.PERSON_IS_LOCATED_IN.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.PERSONS, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.PLACES, extractors.long_() ) ) );
        } );

        // person knows person: Person.id|Person.id|creationDate|
        personKnowsPersonFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new DateTimeDecorator<>(
                                    Knows.CREATION_DATE,
                                    ImportDateUtil.createFor( fromCsvFormat, toNeo4JFormat,
                                            timestampResolution ) ),
                            InputEntityDecorators.defaultRelationshipType(
                                    Rels.KNOWS.name() )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.PERSONS, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.PERSONS, extractors.long_() ),
                    new Header.Entry( Knows.CREATION_DATE, Type.PROPERTY, IndexSpace._, extractors.string() ) ) );
        } );

        // person likes comment: Person.id|Comment.id|creationDate|
        personLikesCommentFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new DateTimeDecorator<>(
                                    Likes.CREATION_DATE,
                                    ImportDateUtil.createFor( fromCsvFormat, toNeo4JFormat,
                                            timestampResolution ) ),
                            InputEntityDecorators.defaultRelationshipType(
                                    Rels.LIKES_COMMENT.name() )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.PERSONS, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.MESSAGES, extractors.long_() ),
                    new Header.Entry( Likes.CREATION_DATE, Type.PROPERTY, IndexSpace._, extractors.string() ) ) );
        } );

        // person likes post: Person.id|Post.id|creationDate|
        personLikesPostFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new DateTimeDecorator<>(
                                    Likes.CREATION_DATE,
                                    ImportDateUtil.createFor( fromCsvFormat, toNeo4JFormat,
                                            timestampResolution ) ),
                            InputEntityDecorators.defaultRelationshipType(
                                    Rels.LIKES_POST.name() )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.PERSONS, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.MESSAGES, extractors.long_() ),
                    new Header.Entry( Likes.CREATION_DATE, Type.PROPERTY, IndexSpace._, extractors.string() ) ) );
        } );

        // person study at organization: Person.id|Organisation.id|classYear|
        personStudyAtOrganisationFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.STUDY_AT.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.PERSONS, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.ORGANIZATIONS, extractors.long_() ),
                    new Header.Entry( StudiesAt.CLASS_YEAR, Type.PROPERTY, IndexSpace._, extractors.int_() ) ) );
        } );

        // person works at organization: Person.id|Organisation.id|workFrom|
        personWorksAtOrganisationFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.WORKS_AT.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.PERSONS, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.ORGANIZATIONS, extractors.long_() ),
                    new Header.Entry( WorksAt.WORK_FROM, Type.PROPERTY, IndexSpace._, extractors.int_() ) ) );
        } );

        // place is part of place: Place.id|Place.id|
        placeIsPartOfPlaceFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.IS_PART_OF.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.PLACES, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.PLACES, extractors.long_() ) ) );
        } );

        // post has creator person: Post.id|Person.id|
        postHasCreatorPersonFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.POST_HAS_CREATOR.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.MESSAGES, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.PERSONS, extractors.long_() ) ) );
        } );

        // post has tag tag: Post.id|Tag.id|
        postHasTagTagFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.POST_HAS_TAG.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.MESSAGES, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.TAGS, extractors.long_() ) ) );
        } );

        // comment has tag tag: Comment.id|Tag.id|
        commentHasTagTagFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.COMMENT_HAS_TAG.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.MESSAGES, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.TAGS, extractors.long_() ) ) );
        } );

        // post is located in place: Post.id|Place.id|
        postIsLocatedInPlaceFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.POST_IS_LOCATED_IN.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.MESSAGES, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.PLACES, extractors.long_() ) ) );
        } );

        // tag class is subclass of tag class: TagClass.id|TagClass.id|
        tagClassIsSubclassOfTagClassFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.IS_SUBCLASS_OF.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.TAG_CLASSES, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.TAG_CLASSES, extractors.long_() ) ) );
        } );

        // tag has type tag class: Tag.id|TagClass.id|
        tagHasTypeTagClassFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.HAS_TYPE.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.TAGS, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.TAG_CLASSES, extractors.long_() ) ) );
        } );

        // organization is located in place: Organisation.id|Place.id|
        organisationIsLocatedInPlaceFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.ORGANISATION_IS_LOCATED_IN.name() ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( null, Type.START_ID, IndexSpace.ORGANIZATIONS, extractors.long_() ),
                    new Header.Entry( null, Type.END_ID, IndexSpace.PLACES, extractors.long_() ) ) );
        } );

        int denseNodeThreshold = 1;
        Configuration batchImporterConfiguration = new LdbcImporterConfig( denseNodeThreshold );

        Input input = new CsvInput(
                nodeDataFactories,
                new LdbcHeaderFactory( nodeHeaders.stream().toArray( Header[]::new ) ),
                relationshipDataFactories,
                new LdbcHeaderFactory( relationshipHeaders.stream().toArray( Header[]::new ) ),
                IdType.INTEGER,
                configuration,
                Collectors.badCollector( System.out, 0 ),
                batchImporterConfiguration.maxNumberOfProcessors(),
                false
        );

        FormattedLogProvider systemOutLogProvider = FormattedLogProvider.toOutputStream( System.out );
        LogService logService = new SimpleLogService( systemOutLogProvider, systemOutLogProvider );
        BatchImporter batchImporter = new ParallelBatchImporter(
                dbDir,
                new DefaultFileSystemAbstraction(),
                batchImporterConfiguration,
                logService,
                ExecutionMonitors.defaultVisible(),
                (null == importerProperties)
                ? Config.defaults()
                : Config.defaults( MapUtils.loadPropertiesToMap( importerProperties ) )
        );

        LOGGER.info( "Loading CSV files" );
        long startTime = System.currentTimeMillis();

        batchImporter.doImport( input );

        long runtime = System.currentTimeMillis() - startTime;
        System.out.println( String.format(
                "Data imported in: %d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes( runtime ),
                TimeUnit.MILLISECONDS.toSeconds( runtime )
                - TimeUnit.MINUTES.toSeconds( TimeUnit.MILLISECONDS.toMinutes( runtime ) ) ) );

        LOGGER.info( "Creating Indexes & Constraints" );
        startTime = System.currentTimeMillis();

        GraphDatabaseService db = Neo4jDb.newDb( dbDir, importerProperties );

        GraphMetadataProxy.writeTo( db, GraphMetadataProxy.createFrom( metadataTracker ) );

        // Create Indexes
        indexer.createTransactional( db );

        runtime = System.currentTimeMillis() - startTime;
        System.out.println( String.format(
                "Indexes built in: %d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes( runtime ),
                TimeUnit.MILLISECONDS.toSeconds( runtime )
                - TimeUnit.MINUTES.toSeconds( TimeUnit.MILLISECONDS.toMinutes( runtime ) ) ) );

        System.out.printf( "Shutting down..." );
        db.shutdown();
        System.out.println( "Done" );
    }
}
