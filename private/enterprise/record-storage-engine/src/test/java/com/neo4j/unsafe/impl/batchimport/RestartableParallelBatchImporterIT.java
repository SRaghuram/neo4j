/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.unsafe.impl.batchimport;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.unsafe.batchinsert.internal.TransactionLogsInitializer;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.BatchImporterFactory;
import org.neo4j.unsafe.impl.batchimport.CountGroupsStage;
import org.neo4j.unsafe.impl.batchimport.DataImporter;
import org.neo4j.unsafe.impl.batchimport.IdMapperPreparationStage;
import org.neo4j.unsafe.impl.batchimport.NodeCountsAndLabelIndexBuildStage;
import org.neo4j.unsafe.impl.batchimport.NodeDegreeCountStage;
import org.neo4j.unsafe.impl.batchimport.NodeFirstGroupStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipCountsStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipGroupStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipLinkbackStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipLinkforwardStage;
import org.neo4j.unsafe.impl.batchimport.ScanAndCacheGroupsStage;
import org.neo4j.unsafe.impl.batchimport.SparseNodeFirstRelationshipStage;
import org.neo4j.unsafe.impl.batchimport.WriteGroupsStage;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors.invisible;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class, RandomExtension.class} )
class RestartableParallelBatchImporterIT
{
    private static final int NODE_COUNT = 100;
    private static final int RELATIONSHIP_COUNT = 10_000;

    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private RandomRule random;
    @Inject
    private TestDirectory directory;
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
        shouldRestartImport( RelationshipCountsStage.NAME, true );
    }

    @Test
    void shouldRestartImportAfterRelationshipCountsEnd() throws Exception
    {
        shouldRestartImport( RelationshipCountsStage.NAME, false );
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
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( directory.databaseDir() );
        try
        {
            input.verify( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    private BatchImporter importer( ExecutionMonitor monitor )
    {
        return BatchImporterFactory.withHighestPriority().instantiate(
              directory.databaseLayout(), fs, null, DEFAULT, NullLogService.getInstance(), monitor,
              EMPTY, Config.defaults(), RecordFormatSelector.defaultFormat(), NO_MONITOR, jobScheduler, Collector.EMPTY, TransactionLogsInitializer.INSTANCE );
    }
}
