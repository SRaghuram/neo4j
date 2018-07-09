/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.unsafe.impl.batchimport;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.proc.ProcessUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.BatchImporterFactory;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static java.lang.Long.max;
import static org.junit.Assert.assertEquals;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;

public class RestartableImportIT
{
    private static final int NODE_COUNT = 100;
    private static final int RELATIONSHIP_COUNT = 10_000;

    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final RandomRule random = new RandomRule();
    private final TestDirectory directory = TestDirectory.testDirectory( fs );

    @Rule
    public final RuleChain rules = RuleChain.outerRule( random ).around( fs ).around( directory );

    @Test( timeout = 100_000 )
    public void shouldFinishDespiteUnfairShutdowns() throws Exception
    {
        File storeDir = directory.directory( "db" );
        File databaseDirectory = new File( storeDir, DataSourceManager.DEFAULT_DATABASE_NAME );
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

            zip( storeDir, new File( directory.directory( "snapshots" ), String.format( "killed-%02d.zip", restartCount ) ) );
            restartCount++;
        }
        while ( process.exitValue() != 0 );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            input( random.seed() ).verify( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    private Process startImportInSeparateProcess( File databaseDirectory ) throws IOException
    {
        ProcessBuilder pb = new ProcessBuilder( ProcessUtil.getJavaExecutable().toString(), "-cp", ProcessUtil.getClassPath(),
                getClass().getCanonicalName(), databaseDirectory.getPath(), Long.toString( random.seed() ) );
        File wd = new File( "target/test-classes" ).getAbsoluteFile();
        Files.createDirectories( wd.toPath() );
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
            if ( !e.getMessage().contains( "already contains data, cannot do import here" ) )
            {
                throw e;
            }
        }
    }

    private static void zip( File directory, File output ) throws IOException
    {
        try ( ZipOutputStream out = new ZipOutputStream( new FileOutputStream( output ) ) )
        {
            addFilesInDirectory( directory, out, "" );
        }
    }

    private static void addFilesInDirectory( File directory, ZipOutputStream out, String path ) throws IOException
    {
        for ( File file : directory.listFiles() )
        {
            addToZip( out, file, path );
        }
    }

    private static void addToZip( ZipOutputStream out, File file, String path ) throws IOException
    {
        if ( file.isDirectory() )
        {
            addFilesInDirectory( file, out, path + file.getName() + "/" );
        }
        else
        {
            ZipEntry entry = new ZipEntry( path + file.getName() );
            entry.setSize( file.length() );
            out.putNextEntry( entry );
            Files.copy( file.toPath(), out );
        }
    }
}
