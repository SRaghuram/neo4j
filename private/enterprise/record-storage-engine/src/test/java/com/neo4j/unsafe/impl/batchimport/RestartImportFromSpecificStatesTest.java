/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.unsafe.impl.batchimport;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.BatchImporterFactory;
import org.neo4j.unsafe.impl.batchimport.DataImporter;
import org.neo4j.unsafe.impl.batchimport.RelationshipCountsStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipLinkbackStage;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;

import static org.junit.Assert.fail;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;

public class RestartImportFromSpecificStatesTest
{
    private static final long NODE_COUNT = 100;
    private static final long RELATIONSHIP_COUNT = 1_000;

    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final RandomRule random = new RandomRule();
    private final TestDirectory directory = TestDirectory.testDirectory( fs );
    private static final JobScheduler jobScheduler = new ThreadPoolJobScheduler();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( random ).around( fs ).around( directory );

    @AfterClass
    public static void tearDown() throws Exception
    {
        jobScheduler.close();
    }

    @Test
    public void shouldContinueFromLinkingState() throws Exception
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
    public void shouldContinueFromCountsState() throws Exception
    {
        // given
        crashImportAt( RelationshipCountsStage.NAME );

        // when
        SimpleRandomizedInput input = input();
        importer( new PanicSpreadingExecutionMonitor( RelationshipLinkbackStage.NAME, true ) ).doImport( input );

        // then good :)
        verifyDb( input );
    }

    private void crashImportAt( String stageName )
    {
        try
        {
            importer( new PanicSpreadingExecutionMonitor( stageName, false ) ).doImport( input() );
            fail( "Should fail, due to the execution monitor spreading panic" );
        }
        catch ( Exception e )
        {
            // good
        }
    }

    private SimpleRandomizedInput input()
    {
        return new SimpleRandomizedInput( random.seed(), NODE_COUNT, RELATIONSHIP_COUNT, 0, 0 );
    }

    private BatchImporter importer( ExecutionMonitor monitor )
    {
        return BatchImporterFactory.withHighestPriority().instantiate(
              directory.databaseLayout(), fs, null, DEFAULT, NullLogService.getInstance(), monitor,
              EMPTY, Config.defaults(), RecordFormatSelector.defaultFormat(), NO_MONITOR, jobScheduler, Collector.EMPTY );
    }

    private void verifyDb( SimpleRandomizedInput input ) throws IOException
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory.databaseDir() );
        try
        {
            input.verify( db );
        }
        finally
        {
            db.shutdown();
        }
    }
}
