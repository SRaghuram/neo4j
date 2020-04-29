/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.batchimport;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static com.neo4j.internal.batchimport.RestartableParallelBatchImporter.FILE_NAME_STATE;
import static java.lang.Long.max;
import static java.lang.ProcessBuilder.Redirect.appendTo;
import static java.lang.String.format;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.internal.batchimport.Configuration.DEFAULT;
import static org.neo4j.internal.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.io.compress.ZipUtils.zip;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.proc.ProcessUtil.getClassPath;
import static org.neo4j.test.proc.ProcessUtil.getJavaExecutable;

@TestDirectoryExtension
@ExtendWith( RandomExtension.class )
class RestartableImportIT
{
    private static final String COMPLETED = "completed";
    private static final int NODE_COUNT = 100;
    private static final int RELATIONSHIP_COUNT = 10_000;

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private RandomRule random;

    @Test
    void shouldFinishDespiteUnfairShutdowns()
    {
        assertTimeoutPreemptively( ofSeconds( 300 ), () ->
        {
            Neo4jLayout neo4jLayout = Neo4jLayout.ofFlat( testDirectory.homeDir() );
            DatabaseLayout dbLayout = neo4jLayout.databaseLayout( DEFAULT_DATABASE_NAME );
            long startTime = System.currentTimeMillis();
            File dbDirectory = dbLayout.databaseDirectory();
            int timeMeasuringImportExitCode = startImportInSeparateProcess( dbDirectory ).waitFor();
            long time = System.currentTimeMillis() - startTime;
            assertEquals( 0, timeMeasuringImportExitCode );
            fs.deleteRecursively( neo4jLayout.homeDirectory() );
            fs.mkdir( neo4jLayout.homeDirectory() );
            Process process;
            int restartCount = 0;
            int exitCode;
            do
            {
                process = startImportInSeparateProcess( dbDirectory );
                long waitTime = max( time / 4, random.nextLong( time ) + time / 20 * restartCount );
                boolean completedOnItsOwn = process.waitFor( waitTime, TimeUnit.MILLISECONDS );
                if ( !completedOnItsOwn )
                {
                    process.destroyForcibly();
                }
                exitCode = process.waitFor();
                if ( completedOnItsOwn )
                {
                    assertEquals( 0, exitCode );
                }

                zip( fs, dbDirectory, new File( testDirectory.directory( "snapshots" ), format( "killed-%02d.zip", restartCount ) ) );

                if ( !completedOnItsOwn && !fs.fileExists( new File( dbDirectory, FILE_NAME_STATE ) ) &&
                        !fs.fileExists( new File( dbDirectory, COMPLETED ) ) )
                {
                    // This is a case which is, by all means, quite the edge case. This is state where an import started, but was killed
                    // immediately afterwards... in the middle of creating the store files. There have been attempts to solve this in the
                    // restartable importer, which works, but there's always some case somewhere else that breaks. This edge case is only
                    // visible in this test and for users it's just this thing where you'll need to clear out your store manually if this happens.
                    File[] files = fs.listFiles( dbDirectory );
                    if ( files != null )
                    {
                        for ( File file : files )
                        {
                            fs.deleteRecursively( file );
                        }
                    }
                }

                restartCount++;
            }
            while ( exitCode != 0 );
            DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( dbLayout ).build();
            GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
            assertTrue( db.isAvailable( TimeUnit.SECONDS.toMillis( 30 ) ) );
            try
            {
                input( random.seed() ).verify( db );
            }
            finally
            {
                managementService.shutdown();
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
        File reportFile = testDirectory.createFile( "testReport" + seed );
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
            File databaseDirectory = new File( args[0] );
            BatchImporterFactory factory = BatchImporterFactory.withHighestPriority();
            factory.instantiate( DatabaseLayout.ofFlat( databaseDirectory ), new DefaultFileSystemAbstraction(), null, PageCacheTracer.NULL, DEFAULT,
                    NullLogService.getInstance(), ExecutionMonitors.invisible(), EMPTY, Config.defaults(), RecordFormatSelector.defaultFormat(),
                    NO_MONITOR, jobScheduler, Collector.EMPTY, TransactionLogInitializer.asLogFilesInitializer(), INSTANCE )
                   .doImport( input( Long.parseLong( args[1] ) ) );
            // Create this file to communicate completion for real
            new File( databaseDirectory, COMPLETED ).createNewFile();
        }
        catch ( IllegalStateException e )
        {
            if ( !e.getMessage().contains( "already contains data, cannot do import here" ) )
            {
                throw e;
            }
            // else we're done actually. This could have happened if the previous process instance completed the import,
            // but was killed before exiting normally itself.
        }
    }
}
