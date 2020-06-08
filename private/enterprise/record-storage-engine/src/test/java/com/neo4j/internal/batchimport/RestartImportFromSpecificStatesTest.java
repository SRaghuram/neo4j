/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.batchimport;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.DataImporter;
import org.neo4j.internal.batchimport.RelationshipCountsAndTypeIndexBuildStage;
import org.neo4j.internal.batchimport.RelationshipLinkbackStage;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.internal.batchimport.Configuration.DEFAULT;
import static org.neo4j.internal.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@Neo4jLayoutExtension
@ExtendWith( RandomExtension.class )
class RestartImportFromSpecificStatesTest
{
    private static final long NODE_COUNT = 100;
    private static final long RELATIONSHIP_COUNT = 1_000;

    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private RandomRule random;
    @Inject
    private DatabaseLayout databaseLayout;
    private static final JobScheduler jobScheduler = new ThreadPoolJobScheduler();

    @AfterAll
    static void tearDown() throws Exception
    {
        jobScheduler.close();
    }

    @Test
    void shouldContinueFromLinkingState() throws Exception
    {
        // given
        crashImportAt( RelationshipLinkbackStage.NAME );

        // when
        SimpleRandomizedInput input = input();
        importer( new PanicSpreadingExecutionMonitor( DataImporter.RELATIONSHIP_IMPORT_NAME, true ) ).doImport( input );

        // then good :)
        verifyDb( input );
    }

    @Test
    void shouldContinueFromCountsState() throws Exception
    {
        // given
        crashImportAt( RelationshipCountsAndTypeIndexBuildStage.NAME );

        // when
        SimpleRandomizedInput input = input();
        importer( new PanicSpreadingExecutionMonitor( RelationshipLinkbackStage.NAME, true ) ).doImport( input );

        // then good :)
        verifyDb( input );
    }

    private void crashImportAt( String stageName )
    {
        assertThrows( Exception.class, () -> importer( new PanicSpreadingExecutionMonitor( stageName, false ) ).doImport( input() ) );
    }

    private SimpleRandomizedInput input()
    {
        return new SimpleRandomizedInput( random.seed(), NODE_COUNT, RELATIONSHIP_COUNT, 0, 0 );
    }

    private BatchImporter importer( ExecutionMonitor monitor )
    {
        BatchImporterFactory factory = BatchImporterFactory.withHighestPriority();
        return factory.instantiate(
                databaseLayout, fs, null, PageCacheTracer.NULL, DEFAULT, NullLogService.getInstance(), monitor, EMPTY,
                Config.defaults(), RecordFormatSelector.defaultFormat(), NO_MONITOR, jobScheduler, Collector.EMPTY,
                TransactionLogInitializer.getLogFilesInitializer(), INSTANCE );
    }

    private void verifyDb( SimpleRandomizedInput input ) throws IOException
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( databaseLayout ).build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            input.verify( db );
        }
        finally
        {
            managementService.shutdown();
        }
    }
}
