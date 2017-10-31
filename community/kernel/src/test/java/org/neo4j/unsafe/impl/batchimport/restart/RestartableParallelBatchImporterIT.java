/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport.restart;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.proc.ProcessUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.CountGroupsStage;
import org.neo4j.unsafe.impl.batchimport.IdMapperPreparationStage;
import org.neo4j.unsafe.impl.batchimport.NodeCountsStage;
import org.neo4j.unsafe.impl.batchimport.NodeDegreeCountStage;
import org.neo4j.unsafe.impl.batchimport.NodeFirstGroupStage;
import org.neo4j.unsafe.impl.batchimport.NodeStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipCountsStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipGroupStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipLinkbackStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipLinkforwardStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipStage;
import org.neo4j.unsafe.impl.batchimport.ScanAndCacheGroupsStage;
import org.neo4j.unsafe.impl.batchimport.SparseNodeFirstRelationshipStage;
import org.neo4j.unsafe.impl.batchimport.WriteGroupsStage;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;

import static org.junit.Assert.fail;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors.invisible;

public class RestartableParallelBatchImporterIT
{
    private static final int NODE_COUNT = 100;
    private static final int RELATIONSHIP_COUNT = 1_000;

    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final RandomRule random = new RandomRule();
    private final TestDirectory directory = TestDirectory.testDirectory( fs );

    @Rule
    public final RuleChain rules = RuleChain.outerRule( random ).around( fs ).around( directory );

    @Test
    public void shouldRestartImportAfterNodeImportStart() throws Exception
    {
        shouldRestartImport( NodeStage.NAME, true );
    }

    @Test
    public void shouldRestartImportAfterNodeImportEnd() throws Exception
    {
        shouldRestartImport( NodeStage.NAME, false );
    }

    @Test
    public void shouldRestartImportAfterIdMapperStart() throws Exception
    {
        shouldRestartImport( IdMapperPreparationStage.NAME, true );
    }

    @Test
    public void shouldRestartImportAfterIdMapperEnd() throws Exception
    {
        shouldRestartImport( IdMapperPreparationStage.NAME, false );
    }

    @Test
    public void shouldRestartImportAfterRelationshipImportStart() throws Exception
    {
        shouldRestartImport( RelationshipStage.NAME, true );
    }

    @Test
    public void shouldRestartImportAfterRelationshipImportEnd() throws Exception
    {
        shouldRestartImport( RelationshipStage.NAME, false );
    }

    @Test
    public void shouldRestartImportAfterNodeDegreesStart() throws Exception
    {
        shouldRestartImport( NodeDegreeCountStage.NAME, true );
    }

    @Test
    public void shouldRestartImportAfterNodeDegreesEnd() throws Exception
    {
        shouldRestartImport( NodeDegreeCountStage.NAME, false );
    }

    @Test
    public void shouldRestartImportAfterRelationshipLinkForwardStart() throws Exception
    {
        shouldRestartImport( RelationshipLinkforwardStage.NAME, true );
    }

    @Test
    public void shouldRestartImportAfterRelationshipLinkForwardEnd() throws Exception
    {
        shouldRestartImport( RelationshipLinkforwardStage.NAME, false );
    }

    @Test
    public void shouldRestartImportAfterRelationshipGroupStart() throws Exception
    {
        shouldRestartImport( RelationshipGroupStage.NAME, true );
    }

    @Test
    public void shouldRestartImportAfterRelationshipGroupEnd() throws Exception
    {
        shouldRestartImport( RelationshipGroupStage.NAME, false );
    }

    @Test
    public void shouldRestartImportAfterSparseeNodeFirstRelationshipStart() throws Exception
    {
        shouldRestartImport( SparseNodeFirstRelationshipStage.NAME, true );
    }

    @Test
    public void shouldRestartImportAfterSparseeNodeFirstRelationshipEnd() throws Exception
    {
        shouldRestartImport( SparseNodeFirstRelationshipStage.NAME, false );
    }

    @Test
    public void shouldRestartImportAfterRelationshipLinkBackwardStart() throws Exception
    {
        shouldRestartImport( RelationshipLinkbackStage.NAME, true );
    }

    @Test
    public void shouldRestartImportAfterRelationshipLinkBackwardEnd() throws Exception
    {
        shouldRestartImport( RelationshipLinkbackStage.NAME, false );
    }

    @Test
    public void shouldRestartImportAfterCountGroupsStart() throws Exception
    {
        shouldRestartImport( CountGroupsStage.NAME, true );
    }

    @Test
    public void shouldRestartImportAfterCountGroupsEnd() throws Exception
    {
        shouldRestartImport( CountGroupsStage.NAME, false );
    }

    @Test
    public void shouldRestartImportAfterScanAndCacheGroupsStart() throws Exception
    {
        shouldRestartImport( ScanAndCacheGroupsStage.NAME, true );
    }

    @Test
    public void shouldRestartImportAfterScanAndCacheGroupsEnd() throws Exception
    {
        shouldRestartImport( ScanAndCacheGroupsStage.NAME, false );
    }

    @Test
    public void shouldRestartImportAfterWriteGroupsStart() throws Exception
    {
        shouldRestartImport( WriteGroupsStage.NAME, true );
    }

    @Test
    public void shouldRestartImportAfterWriteGroupsEnd() throws Exception
    {
        shouldRestartImport( WriteGroupsStage.NAME, false );
    }

    @Test
    public void shouldRestartImportAfterNodeFirstGroupStart() throws Exception
    {
        shouldRestartImport( NodeFirstGroupStage.NAME, true );
    }

    @Test
    public void shouldRestartImportAfterNodeFirstGroupEnd() throws Exception
    {
        shouldRestartImport( NodeFirstGroupStage.NAME, false );
    }

    @Test
    public void shouldRestartImportAfterNodeCountsStart() throws Exception
    {
        shouldRestartImport( NodeCountsStage.NAME, true );
    }

    @Test
    public void shouldRestartImportAfterNodeCountsEnd() throws Exception
    {
        shouldRestartImport( NodeCountsStage.NAME, false );
    }

    @Test
    public void shouldRestartImportAfterRelationshipCountsStart() throws Exception
    {
        shouldRestartImport( RelationshipCountsStage.NAME, true );
    }

    @Test
    public void shouldRestartImportAfterRelationshipCountsEnd() throws Exception
    {
        shouldRestartImport( RelationshipCountsStage.NAME, false );
    }

    @RepeatRule.Repeat(times = 50)
    @Test
    public void shouldFinishDespiteUnfairShutdowns() throws Exception
    {
        long startTime = System.currentTimeMillis();
        importer( invisible() ).doImport( input() );
        long time = System.currentTimeMillis() - startTime;
        fs.deleteRecursively( directory.absolutePath() );
        Process process;
        do
        {

            ProcessBuilder pb = new ProcessBuilder( ProcessUtil.getJavaExecutable().toString(), "-cp", ProcessUtil.getClassPath(),
                    SimpleImportRunningMain.class.getCanonicalName(), directory.absolutePath().getPath(), Long.toString( random.seed() ) );
            File wd = new File( "target/test-classes" ).getAbsoluteFile();
            pb.directory( wd );
            pb.inheritIO();
            process = pb.start();
            process.waitFor( random.nextLong( time ), TimeUnit.MILLISECONDS );
            if ( process.isAlive() )
            {
                process.destroyForcibly();
            }
        }
        while ( process.exitValue() != 0 );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        try
        {
            input().verify( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    private SimpleRandomizedInput input()
    {
        return new SimpleRandomizedInput( random.seed(), NODE_COUNT, RELATIONSHIP_COUNT, 0, 0 );
    }

    private void shouldRestartImport( String stageName, boolean trueForStart ) throws IOException
    {
        try
        {
            importer( new PanicSpreadingExecutionMonitor( stageName, trueForStart ) ).doImport( input() );
            fail( "Should fail, due to the execution monitor spreading panic" );
        }
        catch ( Exception e )
        {
            // good
        }

        // when
        SimpleRandomizedInput input = input();
        importer( invisible() ).doImport( input );

        // then
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
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
        return new RestartableParallelBatchImporter(
              directory.absolutePath(), fs, null, DEFAULT, NullLogService.getInstance(), monitor,
              EMPTY, Config.defaults(), RecordFormatSelector.defaultFormat() );
    }
}
