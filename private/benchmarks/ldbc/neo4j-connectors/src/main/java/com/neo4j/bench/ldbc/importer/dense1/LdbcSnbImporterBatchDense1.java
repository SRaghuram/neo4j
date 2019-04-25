/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer.dense1;

import com.ldbc.driver.DbException;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.connection.GraphMetadataProxy;
import com.neo4j.bench.ldbc.connection.ImportDateUtil;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.CsvFileInserter;
import com.neo4j.bench.ldbc.importer.GraphMetadataTracker;
import com.neo4j.bench.ldbc.importer.LdbcIndexer;
import com.neo4j.bench.ldbc.importer.LdbcSnbImporter;
import com.neo4j.bench.ldbc.importer.tempindex.OffHeapTempIndexFactory;
import com.neo4j.bench.ldbc.importer.tempindex.TempIndexFactory;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class LdbcSnbImporterBatchDense1 extends LdbcSnbImporter
{
    private static final Logger LOGGER = Logger.getLogger( LdbcSnbImporterBatchDense1.class );

    @Override
    public void load(
            File storeDir,
            File csvDataDir,
            File importerPropertiesFile,
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

        LOGGER.info( format( "Source CSV Dir:        %s", csvDataDir ) );
        LOGGER.info( format( "Target DB Dir:         %s", storeDir ) );
        LOGGER.info( format( "Source Date Format:    %s", fromCsvFormat.name() ) );
        LOGGER.info( format( "Target Date Format:    %s", toNeo4JFormat.name() ) );
        LOGGER.info( format( "Timestamp Resolution:  %s", timestampResolution.name() ) );
        LOGGER.info( format( "With Unique:           %s", withUnique ) );
        LOGGER.info( format( "With Mandatory:        %s", withMandatory ) );

        LOGGER.info( format( "Clear DB directory: %s", storeDir ) );
        FileUtils.deleteDirectory( storeDir );

        LOGGER.info( "Instantiating Neo4j BatchInserter" );
        BatchInserter batchInserter = Neo4jDb.newInserter( storeDir, importerPropertiesFile );

        GraphMetadataTracker metadataTracker = new GraphMetadataTracker(
                toNeo4JFormat,
                timestampResolution,
                Neo4jSchema.NEO4J_DENSE_1
        );

        LdbcIndexer indexer = new LdbcIndexer(
                metadataTracker.neo4jSchema(),
                withUnique,
                withMandatory );

        /*
        * CSV Files
        */
        TempIndexFactory tempIndexFactory = new OffHeapTempIndexFactory();

        CsvFileInserters fileInserters = new CsvFileInserters(
                tempIndexFactory,
                batchInserter,
                metadataTracker,
                csvDataDir,
                ImportDateUtil.createFor( fromCsvFormat, toNeo4JFormat, timestampResolution )
        );

        LOGGER.info( "Loading CSV files" );
        long startTime = System.currentTimeMillis();

        // Node (Comment)
        fileInserters.getCommentsInserters().forEach( this::insertFile );
        // Node (Person)
        fileInserters.getPersonsInserters().forEach( this::insertFile );
        // Node (Place)
        fileInserters.getPlacesInserters().forEach( this::insertFile );
        // Node (Post)
        fileInserters.getPostsInserters().forEach( this::insertFile );
        // Node (Organisation)
        fileInserters.getOrganizationsInserters().forEach( this::insertFile );
        // Node (Tag)
        fileInserters.getTagsInserters().forEach( this::insertFile );

        // Relationship (Person, Organisation)
        fileInserters.getPersonStudyAtOrganisationInserters().forEach( this::insertFile );
        // Relationship (Person, Organisation)
        fileInserters.getPersonWorksAtOrganisationInserters().forEach( this::insertFile );
        // Relationship (Comment, Tag)
        fileInserters.getCommentHasTagTagInserters().forEach( this::insertFile );
        // Relationship (Comment, Comment)
        fileInserters.getComment_HasCreator_IsLocatedIn_ReplyOf_Inserters().forEach( this::insertFile );
        // Relationship (Person, Comment)
        fileInserters.getPersonLikesCommentInserters().forEach( this::insertFile );

        // Node (Forum)
        fileInserters.getForumsInserters().forEach( this::insertFile );

        // Relationship (Person, Post)
        fileInserters.getPersonLikesPostInserters().forEach( this::insertFile );
        // Relationship (Post, Person)
        fileInserters.getPost_HasCreator_HasContainer_InLocatedIn_Inserters().forEach( this::insertFile );
        // Relationship (Post, Tag)
        fileInserters.getPostHasTagTagInserters().forEach( this::insertFile );

        // Relationship (Forum, Person)
        fileInserters.getForumHasMemberPersonInserters().forEach( this::insertFile );
        fileInserters.getForumHasMemberWithPostsPersonInserters().forEach( this::insertFile );
        // Relationship (Forum, Person)
        fileInserters.getForumHasModeratorPersonInserters().forEach( this::insertFile );
        // Relationship (Forum, Tag)
        fileInserters.getForumHasTagInserters().forEach( this::insertFile );

        // Node (TagClass)
        fileInserters.getTagClassesInserters().forEach( this::insertFile );

        // Relationship (Tag, TagClass)
        fileInserters.getTagClassIsSubclassOfTagClassInserters().forEach( this::insertFile );
        // Relationship (Tag, TagClass)
        fileInserters.getTagHasTypeTagClassInserters().forEach( this::insertFile );

        // Relationship (Person, Tag)
        fileInserters.getPersonHasInterestTagInserters().forEach( this::insertFile );

        // Relationship (Person, Place)
        fileInserters.getPersonIsLocatedInPlaceInserters().forEach( this::insertFile );
        // Relationship (Person, Person)
        fileInserters.getPersonKnowsPersonInserters().forEach( this::insertFile );
        // Relationship (Place, Place)
        fileInserters.getPlaceIsPartOfPlaceInserters().forEach( this::insertFile );

        // Relationship (Organisation, Place)
        fileInserters.getOrganisationIsLocatedInPlaceInserters().forEach( this::insertFile );

        fileInserters.shutdownAll();

        long runtime = System.currentTimeMillis() - startTime;
        System.out.println( format(
                "Data imported in: %d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes( runtime ),
                TimeUnit.MILLISECONDS.toSeconds( runtime )
                - TimeUnit.MINUTES.toSeconds( TimeUnit.MILLISECONDS.toMinutes( runtime ) ) ) );

        LOGGER.info( "Creating Indexes & Constraints" );
        startTime = System.currentTimeMillis();

        // Create Indexes
        batchInserter.shutdown();

        File dbDir = new File( storeDir, DEFAULT_DATABASE_NAME );
        DatabaseManagementService managementService = Neo4jDb.newDb( dbDir, importerPropertiesFile );
        GraphDatabaseService db = managementService.database( dbDir.getName() );

        indexer.createTransactional( db );

        GraphMetadataProxy.writeTo( db, GraphMetadataProxy.createFrom( metadataTracker ) );

        LdbcIndexer.waitForIndexesToBeOnline( db );

        runtime = System.currentTimeMillis() - startTime;
        System.out.println( format(
                "Indexes built in: %d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes( runtime ),
                TimeUnit.MILLISECONDS.toSeconds( runtime )
                - TimeUnit.MINUTES.toSeconds( TimeUnit.MILLISECONDS.toMinutes( runtime ) ) ) );

        System.out.printf( "Shutting down..." );
        managementService.shutdown();
        System.out.println( "Done" );
    }

    private void insertFile( CsvFileInserter fileInserter )
    {
        try
        {
            int lines = fileInserter.insertAll();
            LOGGER.info( format( "\t%s - %s", fileInserter.getFile().getName(), lines ) );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
