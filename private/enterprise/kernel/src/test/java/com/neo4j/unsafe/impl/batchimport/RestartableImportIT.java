/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.unsafe.impl.batchimport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.unsafe.impl.batchimport.BatchImporterFactory;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static java.lang.Long.max;
import static java.lang.ProcessBuilder.Redirect.appendTo;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.neo4j.io.compress.ZipUtils.zip;
import static org.neo4j.test.proc.ProcessUtil.getClassPath;
import static org.neo4j.test.proc.ProcessUtil.getJavaExecutable;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class, RandomExtension.class} )
class RestartableImportIT
{
    private static final int NODE_COUNT = 100;
    private static final int RELATIONSHIP_COUNT = 10_000;

    @Inject
    private TestDirectory directory;
    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private RandomRule random;

    @Test
    void shouldFinishDespiteUnfairShutdowns() throws Exception
    {
        assertTimeoutPreemptively( ofSeconds( 300 ), () ->
        {
            File storeDir = directory.directory( "db" );
            File databaseDirectory = directory.databaseDir( storeDir );
            long startTime = System.currentTimeMillis();
            int timeMeasuringImportExitCode = startImportInSeparateProcess( databaseDirectory ).waitFor();
            long time = System.currentTimeMillis() - startTime;
            assertEquals( 0, timeMeasuringImportExitCode );
            fs.deleteRecursively( storeDir );
            fs.mkdir( storeDir );
            Process process;
            int restartCount = 0;
            do
            {
                process = startImportInSeparateProcess( databaseDirectory );
                long waitTime = max( time / 4, random.nextLong( time ) + time / 20 * restartCount );
                process.waitFor( waitTime, TimeUnit.MILLISECONDS );
                boolean manuallyDestroyed = false;
                if ( process.isAlive() )
                {
                    process.destroyForcibly();
                    manuallyDestroyed = true;
                }
                int exitCode = process.waitFor();
                if ( !manuallyDestroyed )
                {
                    assertEquals( 0, exitCode );
                }

                zip( fs, storeDir, new File( directory.directory( "snapshots" ), String.format( "killed-%02d.zip", restartCount ) ) );
                restartCount++;
            }
            while ( process.exitValue() != 0 );
            GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( databaseDirectory );
            try
            {
                input( random.seed() ).verify( db );
            }
            finally
            {
                db.shutdown();
            }
        } );
    }

    private Process startImportInSeparateProcess( File databaseDirectory ) throws IOException
    {
        long seed = random.seed();
        ProcessBuilder pb = new ProcessBuilder( getJavaExecutable().toString(), "-cp", getClassPath(),
                getClass().getCanonicalName(), databaseDirectory.getPath(), Long.toString( seed ) );
        File wd = new File( "target/test-classes" ).getAbsoluteFile();
        Files.createDirectories( wd.toPath() );
        File reportFile = directory.createFile( "testReport" + seed );
        return pb.directory( wd )
                 .redirectOutput( appendTo( reportFile ) )
                 .redirectError( appendTo( reportFile ) )
                 .start();
    }

    private static SimpleRandomizedInput input( long seed )
    {
        return new SimpleRandomizedInput( seed, NODE_COUNT, RELATIONSHIP_COUNT, 0, 0 );
    }

    public static void main( String[] args ) throws Exception
    {
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler() )
        {
            BatchImporterFactory.withHighestPriority().instantiate( DatabaseLayout.of( new File( args[0] ) ), new DefaultFileSystemAbstraction(),
                    null, DEFAULT, NullLogService.getInstance(), ExecutionMonitors.invisible(), EMPTY, Config.defaults(),
                    RecordFormatSelector.defaultFormat(), NO_MONITOR, jobScheduler ).doImport( input( Long.parseLong( args[1] ) ) );
        }
        catch ( IllegalStateException e )
        {
            if ( !e.getMessage().contains( "already contains data, cannot do import here" ) )
            {
                throw e;
            }
        }
    }
}
