/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer.dense1;

import com.ldbc.driver.DbException;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.cli.LdbcCli;
import com.neo4j.bench.ldbc.connection.GraphMetadataProxy;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.connection.TimeStampedRelationshipTypesCache;
import com.neo4j.bench.ldbc.importer.CommentHasCreatorAtTimeRelationshipTypeDecorator;
import com.neo4j.bench.ldbc.importer.CommentIsLocatedInAtTimeRelationshipTypeDecorator;
import com.neo4j.bench.ldbc.importer.CommentReplyOfRelationshipTypeDecorator;
import com.neo4j.bench.ldbc.importer.CsvFilesForMerge;
import com.neo4j.bench.ldbc.importer.DateTimeDecorator;
import com.neo4j.bench.ldbc.importer.ForumHasMemberAtTimeRelationshipTypeDecorator;
import com.neo4j.bench.ldbc.importer.ForumHasMemberWithPostsLoader;
import com.neo4j.bench.ldbc.importer.GraphMetadataTracker;
import com.neo4j.bench.ldbc.importer.LabelCamelCaseDecorator;
import com.neo4j.bench.ldbc.importer.LdbcHeaderFactory;
import com.neo4j.bench.ldbc.importer.LdbcImporterConfig;
import com.neo4j.bench.ldbc.importer.LdbcIndexer;
import com.neo4j.bench.ldbc.importer.LdbcSnbImporter;
import com.neo4j.bench.ldbc.importer.PersonDecorator;
import com.neo4j.bench.ldbc.importer.PersonWorkAtYearDecorator;
import com.neo4j.bench.ldbc.importer.PlaceIsPartOfPlaceNullReplacer;
import com.neo4j.bench.ldbc.importer.PostHasCreatorAtTimeRelationshipTypeDecorator;
import com.neo4j.bench.ldbc.importer.PostIsLocatedInAtTimeRelationshipTypeDecorator;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.ParallelBatchImporter;
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
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_0;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.bench.ldbc.Domain.Forum;
import static com.neo4j.bench.ldbc.Domain.HasMember;
import static com.neo4j.bench.ldbc.Domain.Knows;
import static com.neo4j.bench.ldbc.Domain.Likes;
import static com.neo4j.bench.ldbc.Domain.Message;
import static com.neo4j.bench.ldbc.Domain.Nodes;
import static com.neo4j.bench.ldbc.Domain.Organisation;
import static com.neo4j.bench.ldbc.Domain.Person;
import static com.neo4j.bench.ldbc.Domain.Place;
import static com.neo4j.bench.ldbc.Domain.Post;
import static com.neo4j.bench.ldbc.Domain.Rels;
import static com.neo4j.bench.ldbc.Domain.StudiesAt;
import static com.neo4j.bench.ldbc.Domain.Tag;
import static com.neo4j.bench.ldbc.Domain.TagClass;
import static com.neo4j.bench.ldbc.Domain.WorksAt;
import static com.neo4j.bench.ldbc.connection.ImportDateUtil.createFor;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

public class LdbcSnbImporterParallelDense1 extends LdbcSnbImporter
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
        if ( timestampResolution.equals( LdbcDateCodec.Resolution.NOT_APPLICABLE ) )
        {
            throw new DbException( format( "Invalid Timestamp Resolution: %s", timestampResolution.name() ) );
        }

        System.out.println( format( "Source CSV Dir:        %s", csvDataDir ) );
        System.out.println( format( "Target DB Dir:         %s", storeDir ) );
        System.out.println( format( "Source Date Format:    %s", fromCsvFormat.name() ) );
        System.out.println( format( "Target Date Format:    %s", toNeo4JFormat.name() ) );
        System.out.println( format( "Timestamp Resolution:  %s", timestampResolution.name() ) );
        System.out.println( format( "With Unique:           %s", withUnique ) );
        System.out.println( format( "With Mandatory:        %s", withMandatory ) );

        System.out.println( format( "Clear DB directory: %s", storeDir ) );
        FileUtils.deleteDirectory( storeDir );

        TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache =
                new TimeStampedRelationshipTypesCache();

        GraphMetadataTracker metadataTracker = new GraphMetadataTracker(
                toNeo4JFormat,
                timestampResolution,
                Neo4jSchema.NEO4J_DENSE_1
        );

        LdbcIndexer indexer = new LdbcIndexer(
                metadataTracker.neo4jSchema(),
                withUnique,
                withMandatory );

        ForumHasMemberWithPostsLoader.createIn( csvDataDir );

        PlaceIsPartOfPlaceNullReplacer placeIsPartOfPlaceNullReplacer = new PlaceIsPartOfPlaceNullReplacer();
        List<File> placeIsPartOfPlaceFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.PLACE.matcher( path.getFileName().toString() ).matches() )
                .map( Path::toFile )
                .collect( toList() );
        File noNullPlaceIsPartOfPlaceFile = new File( csvDataDir, "no_null_" + CsvFilesForMerge.PLACE );
        placeIsPartOfPlaceNullReplacer.replaceNullsWithSelfReferencingRelationships(
                placeIsPartOfPlaceFiles,
                LdbcCli.CHARSET,
                '|',
                noNullPlaceIsPartOfPlaceFile
        );

        Extractors extractors = new Extractors( ';' );

        Configuration configuration = Configuration
                .newBuilder()
                .withDelimiter( '|' )
                .withArrayDelimiter( ';' )
                .build();

        /*
        *** NODE FILES ***
         */
        List<Path> commentsFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.COMMENT.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> postsFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.POST.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> forumsFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.FORUM.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> organizationsFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.ORGANIZATION.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> personsFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.PERSON.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> placesFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.PLACE.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> tagClassesFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.TAGCLASS.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> tagsFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.TAG.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );

        /*
        *** RELATIONSHIP FILES ***
         */
        List<Path> commentHasCreatorPersonFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.COMMENT.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> commentIsLocatedInPlaceFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.COMMENT.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> commentReplyOfCommentOrPostFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.COMMENT.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> forumContainerOfPostFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.POST.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> forumHasMemberPersonFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.FORUM_HAS_MEMBER_PERSON
                        .matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> forumHasMemberWithPostsPersonFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.FORUM_HAS_MEMBER_WITH_POSTS_PERSON
                        .matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> forumHasModeratorPersonFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.FORUM.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> forumHasTagFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.FORUM_HAS_TAG_TAG.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> personHasInterestTagFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.PERSON_HAS_INTEREST_TAG
                        .matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> personIsLocatedInPlaceFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.PERSON.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> personKnowsPersonFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.PERSON_KNOWS_PERSON
                        .matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> personLikesCommentFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.PERSON_LIKES_COMMENT.matcher( path.getFileName().toString() )
                        .matches() )
                .collect( toList() );
        List<Path> personLikesPostFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.PERSON_LIKES_POST.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> personStudyAtOrganisationFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.PERSON_STUDIES_AT_ORGANISATION
                        .matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> personWorksAtOrganisationFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.PERSON_WORKS_AT_ORGANISATION.matcher( path.getFileName().toString() )
                        .matches() )
                .collect( toList() );
        List<Path> postHasCreatorPersonFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.POST.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> postHasTagTagFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.POST_HAS_TAG_TAG.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> commentHasTagTagFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.COMMENT_HAS_TAG_TAG
                        .matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> postIsLocatedInPlaceFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.POST.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );
        List<Path> organisationIsLocatedInPlaceFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.ORGANIZATION.matcher( path.getFileName().toString() ).matches() )
                .collect( toList() );

        Groups groups = new Groups();
        Group messagesGroup = groups.getOrCreate( "messages_id_space" );
        Group forumsGroup = groups.getOrCreate( "forums_id_space" );
        Group organizationsGroup = groups.getOrCreate( "organizations_id_space" );
        Group personsGroup = groups.getOrCreate( "persons_id_space" );
        Group placesGroup = groups.getOrCreate( "places_id_space" );
        Group tagClassesGroup = groups.getOrCreate( "tag_classes_id_space" );
        Group tagsGroup = groups.getOrCreate( "tags_id_space" );
        Group nonGroup = groups.getOrCreate( "id_spaces_are_only_used_for_identifiers" );

        List<DataFactory> nodeDataFactories = new ArrayList<>();
        List<Header> nodeHeaders = new ArrayList<>();

        // comments: id|creationDate|locationIP|browserUsed|content|length|creator|place|replyOfPost|replyOfComment
        commentsFiles.forEach( path ->
                {
                    nodeDataFactories.add( DataFactories.data(
                            InputEntityDecorators.decorators(
                                    new DateTimeDecorator(
                                            Message.CREATION_DATE,
                                            () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ) ),
                                    InputEntityDecorators
                                            .additiveLabels( new String[]{
                                                    Nodes.Comment.name(),
                                                    Nodes.Message.name()} )
                            ),
                            LdbcCli.CHARSET,
                            path ) );
                    nodeHeaders.add( new Header(
                            new Header.Entry( Message.ID, Type.ID, messagesGroup, extractors.long_() ),
                            new Header.Entry( Message.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Message.LOCATION_IP, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Message.BROWSER_USED, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Message.CONTENT, Type.PROPERTY, nonGroup, extractors.string() ),
                            new Header.Entry( Message.LENGTH, Type.PROPERTY, nonGroup, extractors.int_() ),
                            new Header.Entry( "creator", Type.IGNORE, nonGroup, extractors.string() ),
                            new Header.Entry( "place", Type.IGNORE, nonGroup, extractors.string() ),
                            new Header.Entry( "replyOfPost", Type.IGNORE, nonGroup, extractors.string() ),
                            new Header.Entry( "replyOfComment", Type.IGNORE, nonGroup, extractors.string() ) ) );
                }
        );

        // posts: id|imageFile|creationDate|locationIP|browserUsed|language|content|length|creator|Forum.id|place
        postsFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new DateTimeDecorator(
                                    Message.CREATION_DATE,
                                    () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ) ),
                            InputEntityDecorators
                                    .additiveLabels( new String[]{
                                            Nodes.Post.name(),
                                            Nodes.Message.name()} )
                    ),
                    LdbcCli.CHARSET,
                    path ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Message.ID, Type.ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( Post.IMAGE_FILE, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LOCATION_IP, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Message.BROWSER_USED, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Post.LANGUAGE, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CONTENT, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LENGTH, Type.PROPERTY, nonGroup, extractors.int_() ),
                    new Header.Entry( "creator", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "Forum.id", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "place", Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        // forums: id|title|creationDate|moderator
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
                    path ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Forum.ID, Type.ID, forumsGroup, extractors.long_() ),
                    new Header.Entry( Forum.TITLE, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Forum.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( "moderator", Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        // organizations: id|type|name|url|place
        organizationsFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    new LabelCamelCaseDecorator(),
                    LdbcCli.CHARSET,
                    path ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Organisation.ID, Type.ID, organizationsGroup, extractors.long_() ),
                    new Header.Entry( "type", Type.LABEL, nonGroup, extractors.string() ),
                    new Header.Entry( Organisation.NAME, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( "url", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "place", Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        // persons: id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed|place
        personsFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new PersonDecorator(
                                    () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ) ),
                            InputEntityDecorators.additiveLabels( new String[]{
                                    Nodes.Person.name()} )
                    ),
                    LdbcCli.CHARSET,
                    path ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Person.ID, Type.ID, personsGroup, extractors.long_() ),
                    new Header.Entry( Person.FIRST_NAME, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Person.LAST_NAME, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Person.GENDER, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Person.BIRTHDAY, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Person.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Person.LOCATION_IP, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Person.BROWSER_USED, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( "place", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Person.LANGUAGES, Type.PROPERTY, nonGroup, extractors.stringArray() ),
                    new Header.Entry( Person.EMAIL_ADDRESSES, Type.PROPERTY, nonGroup, extractors.stringArray() ) ) );
        } );

        // places: id|name|url|type|isPartOf
        placesFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    new LabelCamelCaseDecorator(),
                    LdbcCli.CHARSET,
                    path ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Place.ID, Type.ID, placesGroup, extractors.long_() ),
                    new Header.Entry( Place.NAME, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( "url", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "type", Type.LABEL, nonGroup, extractors.string() ),
                    new Header.Entry( "isPartOf", Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        // tag classes: id|name|url|isSubclassOf
        tagClassesFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    InputEntityDecorators.additiveLabels( new String[]{
                            Nodes.TagClass.name()} ),
                    LdbcCli.CHARSET,
                    path ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( "id", Type.ID, tagClassesGroup, extractors.long_() ),
                    new Header.Entry( TagClass.NAME, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( "url", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "isSubclassOf", Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        // tags: id|name|url|hasType
        tagsFiles.forEach( path ->
        {
            nodeDataFactories.add( DataFactories.data(
                    InputEntityDecorators.additiveLabels( new String[]{
                            Nodes.Tag.name()} ),
                    LdbcCli.CHARSET,
                    path ) );
            nodeHeaders.add( new Header(
                    new Header.Entry( Tag.ID, Type.ID, tagsGroup, extractors.long_() ),
                    new Header.Entry( Tag.NAME, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( "url", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "hasType", Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        /*
        *** RELATIONSHIP FILES ***
         */
        List<DataFactory> relationshipDataFactories = new ArrayList<>();
        List<Header> relationshipHeaders = new ArrayList<>();

        // comment has creator person
        // comments: id|creationDate|locationIP|browserUsed|content|length|creator|place|replyOfPost|replyOfComment
        commentHasCreatorPersonFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.COMMENT_HAS_CREATOR.name() ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( Message.ID, Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( Message.CREATION_DATE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LOCATION_IP, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.BROWSER_USED, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CONTENT, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LENGTH, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "creator", Type.END_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "place", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "replyOfPost", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "replyOfComment", Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        // comment has creator person - WITH TIME STAMP
        // comments: id|creationDate|locationIP|browserUsed|content|length|creator|place|replyOfPost|replyOfComment
        commentHasCreatorPersonFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    new CommentHasCreatorAtTimeRelationshipTypeDecorator(
                            () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ),
                            timeStampedRelationshipTypesCache,
                            metadataTracker ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( Message.ID, Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( Message.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LOCATION_IP, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.BROWSER_USED, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CONTENT, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LENGTH, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "creator", Type.END_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "place", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "replyOfPost", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "replyOfComment", Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        // comment is located in place
        // comments: id|creationDate|locationIP|browserUsed|content|length|creator|place|replyOfPost|replyOfComment
        commentIsLocatedInPlaceFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    new CommentIsLocatedInAtTimeRelationshipTypeDecorator(
                            () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ),
                            timeStampedRelationshipTypesCache,
                            metadataTracker ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( Message.ID, Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( Message.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LOCATION_IP, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.BROWSER_USED, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CONTENT, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LENGTH, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "creator", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "place", Type.END_ID, placesGroup, extractors.long_() ),
                    new Header.Entry( "replyOfPost", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "replyOfComment", Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        // comment reply of comment/post
        // comments: id|creationDate|locationIP|browserUsed|content|length|creator|place|replyOfPost|replyOfComment
        commentReplyOfCommentOrPostFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    new CommentReplyOfRelationshipTypeDecorator( messagesGroup ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( Message.ID, Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( Message.CREATION_DATE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LOCATION_IP, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.BROWSER_USED, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CONTENT, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LENGTH, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "creator", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "place", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "replyOfPost", Type.PROPERTY, nonGroup, extractors.long_() ),
                    new Header.Entry( "replyOfComment", Type.PROPERTY, nonGroup, extractors.long_() ) ) );
        } );

        // forum container of post
        // posts: id|imageFile|creationDate|locationIP|browserUsed|language|content|length|creator|Forum.id|place
        forumContainerOfPostFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.CONTAINER_OF.name() ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( Message.ID, Type.END_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( Post.IMAGE_FILE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CREATION_DATE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LOCATION_IP, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.BROWSER_USED, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Post.LANGUAGE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CONTENT, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LENGTH, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "creator", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "Forum.id", Type.START_ID, forumsGroup, extractors.long_() ),
                    new Header.Entry( "place", Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        // forum has member person: Forum.id|Person.id|joinDate
        forumHasMemberPersonFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    new ForumHasMemberAtTimeRelationshipTypeDecorator(
                            () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ),
                            timeStampedRelationshipTypesCache,
                            metadataTracker ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Forum.id", Type.START_ID, forumsGroup, extractors.long_() ),
                    new Header.Entry( "Person.id", Type.END_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( HasMember.JOIN_DATE, Type.PROPERTY, nonGroup, extractors.string() ) ) );
        } );

        // forum has member with posts person: Forum.id|Person.id|joinDate
        forumHasMemberWithPostsPersonFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            new DateTimeDecorator(
                                    HasMember.JOIN_DATE,
                                    () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ) ),
                            InputEntityDecorators.defaultRelationshipType(
                                    Rels.HAS_MEMBER_WITH_POSTS.name() )
                    ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Forum.id", Type.START_ID, forumsGroup, extractors.long_() ),
                    new Header.Entry( "Person.id", Type.END_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( HasMember.JOIN_DATE, Type.PROPERTY, nonGroup, extractors.string() ) ) );
        } );

        // forum has moderator person
        // forums: id|title|creationDate|moderator
        forumHasModeratorPersonFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.HAS_MODERATOR.name() ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( Forum.ID, Type.START_ID, forumsGroup, extractors.long_() ),
                    new Header.Entry( Forum.TITLE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Forum.CREATION_DATE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "moderator", Type.END_ID, personsGroup, extractors.long_() ) ) );
        } );

        // forum has tag: Forum.id|Tag.id
        forumHasTagFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.FORUM_HAS_TAG.name() ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Forum.id", Type.START_ID, forumsGroup, extractors.long_() ),
                    new Header.Entry( "Tag.id", Type.END_ID, tagsGroup, extractors.long_() ) ) );
        } );

        // person has interest tag: Person.id|Tag.id
        personHasInterestTagFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.HAS_INTEREST.name() ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Person.id", Type.START_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Tag.id", Type.END_ID, tagsGroup, extractors.long_() ) ) );
        } );

        // person is located in place
        // persons: id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed|place
        personIsLocatedInPlaceFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.PERSON_IS_LOCATED_IN.name() ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( Person.ID, Type.START_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( Person.FIRST_NAME, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Person.LAST_NAME, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Person.GENDER, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Person.BIRTHDAY, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Person.CREATION_DATE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Person.LOCATION_IP, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Person.BROWSER_USED, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "place", Type.END_ID, placesGroup, extractors.long_() ),
                    new Header.Entry( Person.LANGUAGES, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Person.EMAIL_ADDRESSES, Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        // person knows person: Person.id|Person.id|creationDate
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
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Person.id", Type.START_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Person.id", Type.END_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( Knows.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ) ) );
        } );

        // person likes comment: Person.id|Comment.id|creationDate
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
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Person.id", Type.START_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Comment.id", Type.END_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( Likes.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ) ) );
        } );

        // person likes post: Person.id|Post.id|creationDate
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
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Person.id", Type.START_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Post.id", Type.END_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( Likes.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ) ) );
        } );

        // person study at organization: Person.id|Organisation.id|classYear
        personStudyAtOrganisationFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.STUDY_AT.name() ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Person.id", Type.START_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Organisation.id", Type.END_ID, organizationsGroup, extractors.long_() ),
                    new Header.Entry( StudiesAt.CLASS_YEAR, Type.PROPERTY, nonGroup, extractors.int_() ) ) );
        } );

        // person works at organization: Person.id|Organisation.id|workFrom
        personWorksAtOrganisationFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    new PersonWorkAtYearDecorator(
                            metadataTracker,
                            timeStampedRelationshipTypesCache ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Person.id", Type.START_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Organisation.id", Type.END_ID, organizationsGroup, extractors.long_() ),
                    new Header.Entry( WorksAt.WORK_FROM, Type.PROPERTY, nonGroup, extractors.int_() ) ) );
        } );

        // place is part of place
        // places: id|name|url|type|isPartOf
        relationshipDataFactories.add( DataFactories.data(
                InputEntityDecorators.defaultRelationshipType(
                        Rels.IS_PART_OF.name() ),
                LdbcCli.CHARSET,
                noNullPlaceIsPartOfPlaceFile.toPath() ) );
        relationshipHeaders.add( new Header(
                new Header.Entry( Place.ID, Type.START_ID, placesGroup, extractors.long_() ),
                new Header.Entry( Place.NAME, Type.IGNORE, nonGroup, extractors.string() ),
                new Header.Entry( "url", Type.IGNORE, nonGroup, extractors.string() ),
                new Header.Entry( "type", Type.IGNORE, nonGroup, extractors.string() ),
                new Header.Entry( "isPartOf", Type.END_ID, placesGroup, extractors.long_() ) ) );

        // post has creator person
        // posts: id|imageFile|creationDate|locationIP|browserUsed|language|content|length|creator|Forum.id|place
        postHasCreatorPersonFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.POST_HAS_CREATOR.name() ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( Message.ID, Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( Post.IMAGE_FILE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CREATION_DATE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LOCATION_IP, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.BROWSER_USED, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Post.LANGUAGE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CONTENT, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LENGTH, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "creator", Type.END_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Forum.id", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "place", Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        // post has creator person - WITH TIME STAMP
        // posts: id|imageFile|creationDate|locationIP|browserUsed|language|content|length|creator|Forum.id|place
        postHasCreatorPersonFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    new PostHasCreatorAtTimeRelationshipTypeDecorator(
                            () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ),
                            timeStampedRelationshipTypesCache,
                            metadataTracker ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( Message.ID, Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( Post.IMAGE_FILE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LOCATION_IP, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.BROWSER_USED, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Post.LANGUAGE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CONTENT, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LENGTH, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "creator", Type.END_ID, personsGroup, extractors.long_() ),
                    new Header.Entry( "Forum.id", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "place", Type.IGNORE, nonGroup, extractors.string() ) ) );
        } );

        // post has tag tag: Post.id|Tag.id
        postHasTagTagFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.POST_HAS_TAG.name() ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Post.id", Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( "Tag.id", Type.END_ID, tagsGroup, extractors.long_() ) ) );
        } );

        // comment has tag tag: Comment.id|Tag.id
        commentHasTagTagFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.COMMENT_HAS_TAG.name() ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "Comment.id", Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( "Tag.id", Type.END_ID, tagsGroup, extractors.long_() ) ) );
        } );

        // post is located in place
        // posts: id|imageFile|creationDate|locationIP|browserUsed|language|content|length|creator|Forum.id|place
        postIsLocatedInPlaceFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    new PostIsLocatedInAtTimeRelationshipTypeDecorator(
                            () -> createFor( fromCsvFormat, toNeo4JFormat, timestampResolution ),
                            timeStampedRelationshipTypesCache,
                            metadataTracker ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( Message.ID, Type.START_ID, messagesGroup, extractors.long_() ),
                    new Header.Entry( Post.IMAGE_FILE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CREATION_DATE, Type.PROPERTY, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LOCATION_IP, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.BROWSER_USED, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Post.LANGUAGE, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.CONTENT, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Message.LENGTH, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "creator", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "Forum.id", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "place", Type.END_ID, placesGroup, extractors.long_() ) ) );
        } );

        // tags classes: id|name|url|isSubclassOf
        tagClassesFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.decorators(
                            // TODO remove
                            // new TagClassIsSubClassOfTagClassDecorator(),
                            InputEntityDecorators.defaultRelationshipType(
                                    Rels.IS_SUBCLASS_OF.name() )
                    ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "id", Type.START_ID, tagClassesGroup, extractors.long_() ),
                    new Header.Entry( TagClass.NAME, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "url", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "isSubclassOf", Type.END_ID, tagClassesGroup, extractors.long_() ) ) );
        } );

        // tag has type tag class: id|name|url|hasType|
        tagsFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.HAS_TYPE.name() ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( "id", Type.START_ID, tagsGroup, extractors.long_() ),
                    new Header.Entry( "name", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "url", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "hasType", Type.END_ID, tagClassesGroup, extractors.long_() ) ) );
        } );

        // organization is located in place
        // organizations: id|type|name|url|place
        organisationIsLocatedInPlaceFiles.forEach( path ->
        {
            relationshipDataFactories.add( DataFactories.data(
                    InputEntityDecorators.defaultRelationshipType(
                            Rels.ORGANISATION_IS_LOCATED_IN.name() ),
                    LdbcCli.CHARSET,
                    path ) );
            relationshipHeaders.add( new Header(
                    new Header.Entry( Organisation.ID, Type.START_ID, organizationsGroup, extractors.long_() ),
                    new Header.Entry( "type", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( Organisation.NAME, Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "url", Type.IGNORE, nonGroup, extractors.string() ),
                    new Header.Entry( "place", Type.END_ID, placesGroup, extractors.long_() ) ) );
        } );

        // note: assumes input factories are called in order they are given, and two times: first->last, first->last

        Input input = new CsvInput(
                nodeDataFactories,
                new LdbcHeaderFactory( nodeHeaders.toArray( new Header[0] ) ),
                relationshipDataFactories,
                new LdbcHeaderFactory( relationshipHeaders.toArray( new Header[0] ) ),
                IdType.INTEGER,
                configuration,
                CsvInput.NO_MONITOR,
                INSTANCE
        );

        LogProvider systemOutLogProvider = new Log4jLogProvider( System.out );
        LogService logService = new SimpleLogService( systemOutLogProvider, systemOutLogProvider );
        JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        LifeSupport lifeSupport = new LifeSupport();
        lifeSupport.add( jobScheduler );
        lifeSupport.start();
        Config dbConfig = null == importerProperties ? Config.defaults() : Config.newBuilder().fromFile( importerProperties ).build();
        dbConfig.set( GraphDatabaseSettings.dense_node_threshold, 1 );
        Collector badCollector = Collector.EMPTY;
        BatchImporter batchImporter = new ParallelBatchImporter(
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
        );

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
