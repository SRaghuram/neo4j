/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.unsafe.impl.batchimport;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.proc.ProcessUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static java.lang.Long.max;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.Format.duration;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors.invisible;

public class RestartableImportIT
{
    private static final int NODE_COUNT = 100;
    private static final int RELATIONSHIP_COUNT = 1_000;

    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final RandomRule random = new RandomRule();
    private final TestDirectory directory = TestDirectory.testDirectory( fs );

    @Rule
    public final RuleChain rules = RuleChain.outerRule( random ).around( fs ).around( directory );

    @Test( timeout = 100_000 )
    public void shouldFinishDespiteUnfairShutdowns() throws Exception
    {
        long startTime = System.currentTimeMillis();
        int timeMeasuringImportExitCode = startImportInSeparateProcess().waitFor();
        long time = System.currentTimeMillis() - startTime;
        assertEquals( 0, timeMeasuringImportExitCode );
        fs.deleteRecursively( directory.absolutePath() );
        fs.mkdir( directory.absolutePath() );
        Process process;
        do
        {
            process = startImportInSeparateProcess();
            long waitTime = max( time / 4, random.nextLong( time ) );
            process.waitFor( waitTime, TimeUnit.MILLISECONDS );
            if ( process.isAlive() )
            {
                process.destroyForcibly();
            }
        }
        while ( process.exitValue() != 0 );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        try
        {
            input( random.seed() ).verify( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    private Process startImportInSeparateProcess() throws IOException
    {
        ProcessBuilder pb = new ProcessBuilder( ProcessUtil.getJavaExecutable().toString(), "-cp", ProcessUtil.getClassPath(),
                getClass().getCanonicalName(), directory.absolutePath().getPath(), Long.toString( random.seed() ) );
        File wd = new File( "target/test-classes" ).getAbsoluteFile();
        pb.directory( wd );
        pb.inheritIO();
        return pb.start();
    }

    private static SimpleRandomizedInput input( long seed )
    {
        return new SimpleRandomizedInput( seed, NODE_COUNT, RELATIONSHIP_COUNT, 0, 0 );
    }

    public static void main( String[] args ) throws IOException
    {
        try
        {
            BatchImporterFactory.withHighestPriority().instantiate( new File( args[0] ), new DefaultFileSystemAbstraction(),
                    null, DEFAULT, NullLogService.getInstance(),
                    ExecutionMonitors.invisible(), EMPTY, Config.defaults(), RecordFormatSelector.defaultFormat(), NO_MONITOR ).doImport(
                    input( Long.parseLong( args[1] ) ) );
        }
        catch ( IllegalStateException e )
        {
            if ( e.getMessage().contains( "already contains data, cannot do import here" ) )
            {   // In this test, this exception is OK
            }
            else
            {
                throw e;
            }
        }
    }
}
