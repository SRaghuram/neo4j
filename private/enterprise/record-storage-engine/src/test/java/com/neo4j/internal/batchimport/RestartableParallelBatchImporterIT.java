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

import org.neo4j.batchinsert.internal.TransactionLogsInitializer;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.CountGroupsStage;
import org.neo4j.internal.batchimport.DataImporter;
import org.neo4j.internal.batchimport.IdMapperPreparationStage;
import org.neo4j.internal.batchimport.NodeCountsAndLabelIndexBuildStage;
import org.neo4j.internal.batchimport.NodeDegreeCountStage;
import org.neo4j.internal.batchimport.NodeFirstGroupStage;
import org.neo4j.internal.batchimport.RelationshipCountsAndTypeIndexBuildStage;
import org.neo4j.internal.batchimport.RelationshipGroupStage;
import org.neo4j.internal.batchimport.RelationshipLinkbackStage;
import org.neo4j.internal.batchimport.RelationshipLinkforwardStage;
import org.neo4j.internal.batchimport.ScanAndCacheGroupsStage;
import org.neo4j.internal.batchimport.SparseNodeFirstRelationshipStage;
import org.neo4j.internal.batchimport.WriteGroupsStage;
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
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.internal.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.internal.batchimport.Configuration.DEFAULT;
import static org.neo4j.internal.batchimport.BaseImportLogic.NO_MONITOR;
import static org.neo4j.internal.batchimport.staging.ExecutionMonitors.invisible;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@Neo4jLayoutExtension
@ExtendWith( RandomExtension.class )
class RestartableParallelBatchImporterIT
{
    private static final int NODE_COUNT = 100;
    private static final int RELATIONSHIP_COUNT = 10_000;

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
    void shouldRestartImportAfterNodeImportStart() throws Exception
    {
        shouldRestartImport( DataImporter.NODE_IMPORT_NAME, true );
    }

    @Test
    void shouldRestartImportAfterNodeImportEnd() throws Exception
    {
        shouldRestartImport( DataImporter.NODE_IMPORT_NAME, false );
    }

    @Test
    void shouldRestartImportAfterIdMapperStart() throws Exception
    {
        shouldRestartImport( IdMapperPreparationStage.NAME, true );
    }

    @Test
    void shouldRestartImportAfterIdMapperEnd() throws Exception
    {
        shouldRestartImport( IdMapperPreparationStage.NAME, false );
    }

    @Test
    void shouldRestartImportAfterRelationshipImportStart() throws Exception
    {
        shouldRestartImport( DataImporter.RELATIONSHIP_IMPORT_NAME, true );
    }

    @Test
    void shouldRestartImportAfterRelationshipImportEnd() throws Exception
    {
        shouldRestartImport( DataImporter.RELATIONSHIP_IMPORT_NAME, false );
    }

    @Test
    void shouldRestartImportAfterNodeDegreesStart() throws Exception
    {
        shouldRestartImport( NodeDegreeCountStage.NAME, true );
    }

    @Test
    void shouldRestartImportAfterNodeDegreesEnd() throws Exception
    {
        shouldRestartImport( NodeDegreeCountStage.NAME, false );
    }

    @Test
    void shouldRestartImportAfterRelationshipLinkForwardStart() throws Exception
    {
        shouldRestartImport( RelationshipLinkforwardStage.NAME, true );
    }

    @Test
    void shouldRestartImportAfterRelationshipLinkForwardEnd() throws Exception
    {
        shouldRestartImport( RelationshipLinkforwardStage.NAME, false );
    }

    @Test
    void shouldRestartImportAfterRelationshipGroupStart() throws Exception
    {
        shouldRestartImport( RelationshipGroupStage.NAME, true );
    }

    @Test
    void shouldRestartImportAfterRelationshipGroupEnd() throws Exception
    {
        shouldRestartImport( RelationshipGroupStage.NAME, false );
    }

    @Test
    void shouldRestartImportAfterSparseeNodeFirstRelationshipStart() throws Exception
    {
        shouldRestartImport( SparseNodeFirstRelationshipStage.NAME, true );
    }

    @Test
    void shouldRestartImportAfterSparseeNodeFirstRelationshipEnd() throws Exception
    {
        shouldRestartImport( SparseNodeFirstRelationshipStage.NAME, false );
    }

    @Test
    void shouldRestartImportAfterRelationshipLinkBackwardStart() throws Exception
    {
        shouldRestartImport( RelationshipLinkbackStage.NAME, true );
    }

    @Test
    void shouldRestartImportAfterRelationshipLinkBackwardEnd() throws Exception
    {
        shouldRestartImport( RelationshipLinkbackStage.NAME, false );
    }

    @Test
    void shouldRestartImportAfterCountGroupsStart() throws Exception
    {
        shouldRestartImport( CountGroupsStage.NAME, true );
    }

    @Test
    void shouldRestartImportAfterCountGroupsEnd() throws Exception
    {
        shouldRestartImport( CountGroupsStage.NAME, false );
    }

    @Test
    void shouldRestartImportAfterScanAndCacheGroupsStart() throws Exception
    {
        shouldRestartImport( ScanAndCacheGroupsStage.NAME, true );
    }

    @Test
    void shouldRestartImportAfterScanAndCacheGroupsEnd() throws Exception
    {
        shouldRestartImport( ScanAndCacheGroupsStage.NAME, false );
    }

    @Test
    void shouldRestartImportAfterWriteGroupsStart() throws Exception
    {
        shouldRestartImport( WriteGroupsStage.NAME, true );
    }

    @Test
    void shouldRestartImportAfterWriteGroupsEnd() throws Exception
    {
        shouldRestartImport( WriteGroupsStage.NAME, false );
    }

    @Test
    void shouldRestartImportAfterNodeFirstGroupStart() throws Exception
    {
        shouldRestartImport( NodeFirstGroupStage.NAME, true );
    }

    @Test
    void shouldRestartImportAfterNodeFirstGroupEnd() throws Exception
    {
        shouldRestartImport( NodeFirstGroupStage.NAME, false );
    }

    @Test
    void shouldRestartImportAfterNodeCountsStart() throws Exception
    {
        shouldRestartImport( NodeCountsAndLabelIndexBuildStage.NAME, true );
    }

    @Test
    void shouldRestartImportAfterNodeCountsEnd() throws Exception
    {
        shouldRestartImport( NodeCountsAndLabelIndexBuildStage.NAME, false );
    }

    @Test
    void shouldRestartImportAfterRelationshipCountsStart() throws Exception
    {
        shouldRestartImport( RelationshipCountsAndTypeIndexBuildStage.NAME, true );
    }

    @Test
    void shouldRestartImportAfterRelationshipCountsEnd() throws Exception
    {
        shouldRestartImport( RelationshipCountsAndTypeIndexBuildStage.NAME, false );
    }

    private SimpleRandomizedInput input()
    {
        return new SimpleRandomizedInput( random.seed(), NODE_COUNT, RELATIONSHIP_COUNT, 0, 0 );
    }

    private void shouldRestartImport( String stageName, boolean trueForStart ) throws IOException
    {
        assertThrows( Exception.class, () -> importer( new PanicSpreadingExecutionMonitor( stageName, trueForStart ) ).doImport( input() ) );

        // when
        SimpleRandomizedInput input = input();
        importer( invisible() ).doImport( input );

        // then
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

    private BatchImporter importer( ExecutionMonitor monitor ) throws IOException
    {
        return BatchImporterFactory.withHighestPriority().instantiate( databaseLayout, fs, null, PageCacheTracer.NULL,
                DEFAULT, NullLogService.getInstance(), monitor, EMPTY,
                Config.defaults( preallocate_logical_logs, false ),
                //RecordFormatSelector.defaultFormat(),
                NO_MONITOR, jobScheduler, Collector.EMPTY,
                TransactionLogsInitializer.INSTANCE, INSTANCE );
    }
}
