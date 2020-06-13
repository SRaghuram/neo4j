/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer.regular;

import com.ldbc.driver.DbException;
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
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.CsvFiles;
import com.neo4j.bench.ldbc.importer.DateTimeDecorator;
import com.neo4j.bench.ldbc.importer.GraphMetadataTracker;
import com.neo4j.bench.ldbc.importer.LabelCamelCaseDecorator;
import com.neo4j.bench.ldbc.importer.LdbcHeaderFactory;
import com.neo4j.bench.ldbc.importer.LdbcImporterConfig;
import com.neo4j.bench.ldbc.importer.LdbcIndexer;
import com.neo4j.bench.ldbc.importer.LdbcSnbImporter;
import com.neo4j.bench.ldbc.importer.PersonDecorator;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.batchimport.*;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.InputEntityDecorators;
import org.neo4j.internal.batchimport.input.csv.CsvInput;
import org.neo4j.internal.batchimport.input.csv.DataFactories;
import org.neo4j.internal.batchimport.input.csv.DataFactory;
import org.neo4j.internal.batchimport.input.csv.Header;
import org.neo4j.internal.batchimport.input.csv.Type;
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_0;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.bench.ldbc.connection.ImportDateUtil.createFor;
import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

public class LdbcSnbImporterParallelRegular extends LdbcSnbImporter
{
    @Override
    public void load(
            File storeDir,
            File csvDataDir,
            File importerProperties,
            LdbcDateCodec.Format fromCsvFormat,
            LdbcDateCodec.Format toNeo4JFormat,
            LdbcDateCodec.Resolution timestampResolution,
            boolean withUnique,
            boolean withMandatory ) throws IOException, DbException
    {
        System.out.println( format( "Source CSV Dir:        %s", csvDataDir ) );
        System.out.println( format( "Target DB Dir:         %s", storeDir ) );
        System.out.println( format( "Source Date Format:    %s", fromCsvFormat.name() ) );
        System.out.println( format( "Target Date Format:    %s", toNeo4JFormat.name() ) );
        System.out.println( format( "Timestamp Resolution:  %s", timestampResolution.name() ) );
        System.out.println( format( "With Unique:           %s", withUnique ) );
        System.out.println( format( "With Mandatory:        %s", withMandatory ) );

        System.out.println( format( "Clear DB directory: %s", storeDir ) );
        FileUtils.deleteDirectory( storeDir );
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

        Configuration configuration = Configuration.newBuilder()
            .withDelimiter( '|' )
            .withArrayDelimiter( ';' )
            .build();

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
                .filter( path -> CsvFiles.COMMENT_HAS_CREATOR_PERSON.matcher( path.getFileName().toString() )
                        .matches() );
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
                .filter( path -> CsvFiles.FORUM_HAS_MODERATOR_PERSON.matcher( path.getFileName().toString() )
                        .matches() );
        Stream<Path> forumHasTagFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.FORUM_HAS_TAG_TAG.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> personHasInterestTagFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON_HAS_INTEREST_TAG.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> personIsLocatedInPlaceFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON_IS_LOCATED_IN_PLACE.matcher( path.getFileName().toString() )
                        .matches() );
        Stream<Path> personKnowsPersonFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON_KNOWS_PERSON.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> personLikesCommentFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON_LIKES_COMMENT.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> personLikesPostFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON_LIKES_POST.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> personStudyAtOrganisationFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON_STUDIES_AT_ORGANISATION.matcher( path.getFileName().toString() )
                        .matches() );
        Stream<Path> personWorksAtOrganisationFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.PERSON_WORKS_AT_ORGANISATION.matcher( path.getFileName().toString() )
                        .matches() );
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
                .filter( path -> CsvFiles.TAGCLASS_IS_SUBCLASS_OF_TAGCLASS.matcher( path.getFileName().toString() )
                        .matches() );
        Stream<Path> tagHasTypeTagClassFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.TAG_HAS_TYPE_TAGCLASS.matcher( path.getFileName().toString() ).matches() );
        Stream<Path> organisationIsLocatedInPlaceFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFiles.ORGANISATION_IS_LOCATED_IN_PLACE.matcher( path.getFileName().toString() )
                        .matches() );

        Groups groups = new Groups();
        Group messagesGroup = groups.getOrCreate( "messages_id_space" );
        Group forumsGroup = groups.getOrCreate( "forums_id_space" );
        Group organizationsGroup = groups.getOrCreate( "organizations_id_space" );
        Group personsGroup = groups.getOrCreate( "persons_id_space" );
        Group placesGroup = groups.getOrCreate( "places_id_space" );
        Group tagClassesGroup = groups.getOrCreate( "tag_classes_id_space" );
        Group tagsGroup = groups.getOrCreate( "tags_id_space" );
        Group nonGroup = groups.getOrCreate( "id_spaces_are_only_used_for_identifiers" );

        /*
        ***NODE FILES ***
         */
        List<DataFactory> nodeDataFactories = new ArrayList<>();
        List<Header> nodeHeaders = new ArrayList<>();

        // comments: id|creationDate|locationIP|browserUsed|content|length|
        commentsFiles.forEach( path ->
                {
                    nodeDataFactories.add( DataFactories.data(
                            InputEntityDecorators.decorators(
                                    new DateTimeDecorator(
                                            Message.CREATION_DATE,
                                            () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ) ),
                                    InputEntityDecorators.additiveLabels( new String[]{
                                            Nodes.Comment.name(),
                                            Nodes.Message.name()} )
                            ),
                            LdbcCli.CHARSET,
                            path.toFile()
                    ) );
                    nodeHeaders.add( new Header(
                            new Header.Entry( Message.ID, Type.ID, messagesGroup, extractors.long_() ),
                            new Header.Entry( Message.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Message.LOCATION_IP, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Message.BROWSER_USED, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Message.CONTENT, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Message.LENGTH, Type.PROPERTY, nonGroup, extractors.int_() )

                    ) );
                }
        );

        // posts: id|imageFile|creationDate|locationIP|browserUsed|language|content|length|
        postsFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new DateTimeDecorator(
                                    Message.CREATION_DATE,
                                    () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ) ),
                            InputEntityDecorators.additiveLabels( new String[]{
                                    Nodes.Post.name(),
                                    Nodes.Message.name()} )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Message.ID, Type.ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( Post.IMAGE_FILE, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LOCATION_IP, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Message.BROWSER_USED, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Post.LANGUAGE, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CONTENT, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LENGTH, Type.PROPERTY, nonGroup, extractors.int_() ) ) );
        } );

        // forums: id|title|creationDate|
        forumsFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new DateTimeDecorator(
                                    Forum.CREATION_DATE,
                                    () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ) ),
                            InputEntityDecorators.additiveLabels( new String[]{
                                    Nodes.Forum.name()} )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Forum.ID, Type.ID, forumsGroup, extractors.long_() ),
                    new Header.Entry( Forum.TITLE, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Forum.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ) ) );
        } );

        // organizations: id|type|name|url|
        organizationsFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    new LabelCamelCaseDecorator(),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Organisation.ID, Type.ID, organizationsGroup, extractors.long_() ),
                    new Header.Entry( "type", Type.LABEL, nonGroup, extractors.string() ),
                    new Header.Entry( Organisation.NAME, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( "url", Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        // persons: id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed|languages|emails
        personsFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new PersonDecorator(
                                    () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution )
                            ),
                            InputEntityDecorators.additiveLabels( new String[]{
                                    Nodes.Person.name()} )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            nodeHeaders.add(
                    new Header(
                            new Header.Entry( Person.ID, Type.ID, personsGroup, extractors.long_() ),
                            new Header.Entry( Person.FIRST_NAME, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Person.LAST_NAME, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Person.GENDER, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Person.BIRTHDAY, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Person.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Person.LOCATION_IP, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Person.BROWSER_USED, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Person.LANGUAGES, Type.PROPERTY, nonGroup, extractors.stringArray() ),
                            new Header.Entry( Person.EMAIL_ADDRESSES,
                                    Type.PROPERTY, nonGroup, extractors.stringArray() ) ) );
        } );

        // places: id|name|url|type|
        placesFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    new LabelCamelCaseDecorator(),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Place.ID, Type.ID, placesGroup, extractors.long_() ),
                    new Header.Entry( Place.NAME, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( "url", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "type", Type.LABEL, nonGroup, extractors.string() ) ) );
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
                    new Header.Entry( "id", Type.ID, tagClassesGroup, extractors.long_() ),
                    new Header.Entry( TagClass.NAME, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( "url", Type.IGNORE, nonGroup, extractors.string() ) ) );
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
                    new Header.Entry( Tag.ID, Type.ID, tagsGroup, extractors.long_() ),
                    new Header.Entry( Tag.NAME, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( "url", Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        /*
        *** RELATIONSHIP FILES ***
         */
        List<DataFactory> relationshipDataFactories = new ArrayList<>();
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
                    new Header.Entry( "Comment.id", Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( "Person.id", Type.END_ID, personsGroup, extractors.long_() ) ) );
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
                    new Header.Entry( "Comment.id", Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( "Place.id", Type.END_ID, placesGroup, extractors.long_() ) ) );
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
                    new Header.Entry( "Comment.id", Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( "Comment.id", Type.END_ID, messagesGroup, extractors.long_() ) ) );
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
                    new Header.Entry( "Comment.id", Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( "Post.id", Type.END_ID, messagesGroup, extractors.long_() )
            ) );
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
                    new Header.Entry( "Forum.id", Type.START_ID, forumsGroup, extractors.long_() ),
                    new Header.Entry( "Post.id", Type.END_ID, messagesGroup, extractors.long_() )
            ) );
        } );

        // forum has member person: Forum.id|Person.id|joinDate|
        forumHasMemberPersonFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new DateTimeDecorator(
                                    HasMember.JOIN_DATE,
                                    () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ) ),
                            InputEntityDecorators.defaultRelationshipType(
                                    Rels.HAS_MEMBER.name() )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Forum.id", Type.START_ID, forumsGroup, extractors.long_() ),
                    new Header.Entry( "Person.id", Type.END_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( HasMember.JOIN_DATE, Type.PROPERTY, nonGroup, extractors.string() )
            ) );
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
                    new Header.Entry( "Forum.id", Type.START_ID, forumsGroup, extractors.long_() ),
                    new Header.Entry( "Person.id", Type.END_ID, personsGroup, extractors.long_() ) ) );
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
                    new Header.Entry( "Forum.id", Type.START_ID, forumsGroup, extractors.long_() ),
                    new Header.Entry( "Tag.ig", Type.END_ID, tagsGroup, extractors.long_() ) ) );
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
                    new Header.Entry( "Person.id", Type.START_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Tag.id", Type.END_ID, tagsGroup, extractors.long_() )
            ) );
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
                    new Header.Entry( "Person.id", Type.START_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Place.id", Type.END_ID, placesGroup, extractors.long_() ) ) );
        } );

        // person knows person: Person.id|Person.id|creationDate|
        personKnowsPersonFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new DateTimeDecorator(
                                    Knows.CREATION_DATE,
                                    () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ) ),
                            InputEntityDecorators.defaultRelationshipType(
                                    Rels.KNOWS.name() )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Person.id", Type.START_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Person.id", Type.END_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( Knows.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ) ) );
        } );

        // person likes comment: Person.id|Comment.id|creationDate|
        personLikesCommentFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new DateTimeDecorator(
                                    Likes.CREATION_DATE,
                                    () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ) ),
                            InputEntityDecorators.defaultRelationshipType(
                                    Rels.LIKES_COMMENT.name() )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Person.id", Type.START_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Comment.id", Type.END_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( Likes.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ) ) );
        } );

        // person likes post: Person.id|Post.id|creationDate|
        personLikesPostFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new DateTimeDecorator(
                                    Likes.CREATION_DATE,
                                    () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ) ),
                            InputEntityDecorators.defaultRelationshipType(
                                    Rels.LIKES_POST.name() )
                    ),
                    LdbcCli.CHARSET,
                    path.toFile() ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Person.id", Type.START_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Post.id", Type.END_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( Likes.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ) ) );
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
                    new Header.Entry( "Person.id", Type.START_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Organisation.id", Type.END_ID, organizationsGroup, extractors.long_() ),
                    new Header.Entry( StudiesAt.CLASS_YEAR, Type.PROPERTY, nonGroup, extractors.int_() ) ) );
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
                    new Header.Entry( "Person.id", Type.START_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Organisation.id", Type.END_ID, organizationsGroup, extractors.long_() ),
                    new Header.Entry( WorksAt.WORK_FROM, Type.PROPERTY, nonGroup, extractors.int_() ) ) );
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
                    new Header.Entry( "Place.id", Type.START_ID, placesGroup, extractors.long_() ),
                    new Header.Entry( "Place.id", Type.END_ID, placesGroup, extractors.long_() ) ) );
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
                    new Header.Entry( "Post.id", Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( "Place.id", Type.END_ID, personsGroup, extractors.long_() ) ) );
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
                    new Header.Entry( "Post.id", Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( "Tag.id", Type.END_ID, tagsGroup, extractors.long_() ) ) );
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
                    new Header.Entry( "Comment.id", Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( "Tag.id", Type.END_ID, tagsGroup, extractors.long_() ) ) );
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
                    new Header.Entry( "Post.id", Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( "Place.id", Type.END_ID, placesGroup, extractors.long_() ) ) );
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
                    new Header.Entry( "TagClass.id", Type.START_ID, tagClassesGroup, extractors.long_() ),
                    new Header.Entry( "TagClass.id", Type.END_ID, tagClassesGroup, extractors.long_() ) ) );
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
                    new Header.Entry( "Tag.id", Type.START_ID, tagsGroup, extractors.long_() ),
                    new Header.Entry( "TagClass.id", Type.END_ID, tagClassesGroup, extractors.long_() ) ) );
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
                    new Header.Entry( "Organisation.id", Type.START_ID, organizationsGroup, extractors.long_() ),
                    new Header.Entry( "Place.id", Type.END_ID, placesGroup, extractors.long_() ) ) );
        } );

        Input input = new CsvInput(
                nodeDataFactories,
                new LdbcHeaderFactory( nodeHeaders.stream().toArray( Header[]::new ) ),
                relationshipDataFactories,
                new LdbcHeaderFactory( relationshipHeaders.stream().toArray( Header[]::new ) ),
                IdType.INTEGER,
                configuration,
                CsvInput.NO_MONITOR,
                INSTANCE
        );

        FormattedLogProvider systemOutLogProvider = FormattedLogProvider.toOutputStream( System.out );
        LogService logService = new SimpleLogService( systemOutLogProvider, systemOutLogProvider );
        JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        LifeSupport lifeSupport = new LifeSupport();
        lifeSupport.add( jobScheduler );
        lifeSupport.start();
        Config dbConfig = null == importerProperties ? Config.defaults() : Config.newBuilder().fromFile( importerProperties ).build();
        dbConfig.set( GraphDatabaseSettings.dense_node_threshold, 1 );
        Collector badCollector = Collector.EMPTY;
        DatabaseLayout databaseLayout = Neo4jDb.layoutWithTxLogLocation( storeDir );
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        org.neo4j.internal.batchimport.Configuration config = new LdbcImporterConfig();
        BatchingNeoStores store = StandardBatchImporterFactory.instantiateNeoStores( fs, databaseLayout,
                null, PageCacheTracer.NULL, config, logService, AdditionalInitialIds.EMPTY, dbConfig, jobScheduler, INSTANCE );
        ImportLogic importLogic = new ImportLogic( fs, databaseLayout, store, config, dbConfig, logService,
                ExecutionMonitors.defaultVisible(), badCollector, NO_MONITOR, PageCacheTracer.NULL, INSTANCE );

        BatchImporter batchImporter = new ParallelBatchImporter(databaseLayout, fs, null, PageCacheTracer.NULL, config,
                logService, ExecutionMonitors.defaultVisible(), AdditionalInitialIds.EMPTY,
                dbConfig, importLogic, store,
                NO_MONITOR, jobScheduler, badCollector, TransactionLogInitializer.getLogFilesInitializer1(), INSTANCE);

        /*BatchImporter batchImporter = new ParallelBatchImporter(
                Neo4jDb.layoutWithTxLogLocation( storeDir ),
                new DefaultFileSystemAbstraction(),
                null,
                PageCacheTracer.NULL,
                new LdbcImporterConfig(),
                logService,
                ExecutionMonitors.defaultVisible(),
                AdditionalInitialIds.EMPTY,
                dbConfig,
                StandardV4_0.RECORD_FORMATS,
                NO_MONITOR,
                jobScheduler,
                badCollector,
                TransactionLogInitializer.getLogFilesInitializer(),
                INSTANCE
        );*/

        System.out.println( "Loading CSV files" );
        long startTime = System.currentTimeMillis();

        batchImporter.doImport( input );
        badCollector.close();
        lifeSupport.shutdown();

        long runtime = System.currentTimeMillis() - startTime;
        System.out.println( String.format(
                "Data imported in: %d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes( runtime ),
                TimeUnit.MILLISECONDS.toSeconds( runtime )
                - TimeUnit.MINUTES.toSeconds( TimeUnit.MILLISECONDS.toMinutes( runtime ) ) ) );

        System.out.println( "Creating Indexes & Constraints" );
        startTime = System.currentTimeMillis();

        DatabaseManagementService managementService = Neo4jDb.newDb( storeDir, importerProperties );
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );

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
        managementService.shutdown();
        System.out.println( "Done" );
    }
}
