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
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.proc.ProcessUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;

import static java.lang.Long.max;

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
        importer( invisible() ).doImport( input() );
        long time = System.currentTimeMillis() - startTime;
        fs.deleteRecursively( directory.absolutePath() );
        fs.mkdir( directory.absolutePath() );
        Process process;
        do
        {
            ProcessBuilder pb = new ProcessBuilder( ProcessUtil.getJavaExecutable().toString(), "-cp", ProcessUtil.getClassPath(),
                    SimpleImportRunningMain.class.getCanonicalName(), directory.absolutePath().getPath(), Long.toString( random.seed() ) );
            File wd = new File( "target/test-classes" ).getAbsoluteFile();
            pb.directory( wd );
            pb.inheritIO();
            process = pb.start();
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
            input().verify( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    private BatchImporter importer( ExecutionMonitor monitor )
    {
        return BatchImporterFactory.withHighestPriority().instantiate(
              directory.absolutePath(), fs, null, DEFAULT, NullLogService.getInstance(), monitor,
              EMPTY, Config.defaults(), RecordFormatSelector.defaultFormat(), NO_MONITOR );
    }

    private SimpleRandomizedInput input()
    {
        return new SimpleRandomizedInput( random.seed(), NODE_COUNT, RELATIONSHIP_COUNT, 0, 0 );
    }
}
