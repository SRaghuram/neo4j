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

import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
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

    @Test( timeout = 100000 )
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

    private BatchImporter importer( ExecutionMonitor monitor )
    {
        return new RestartableParallelBatchImporter(
              directory.absolutePath(), fs, null, DEFAULT, NullLogService.getInstance(), monitor,
              EMPTY, Config.defaults(), RecordFormatSelector.defaultFormat() );
    }

    private SimpleRandomizedInput input()
    {
        return new SimpleRandomizedInput( random.seed(), NODE_COUNT, RELATIONSHIP_COUNT, 0, 0 );
    }
}
