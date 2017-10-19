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

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.RelationshipCountsStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipLinkbackStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipStage;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;

import static org.junit.Assert.fail;

import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.restart.SimpleRandomizedInput.randomizedInput;

public class RestartImportFromSpecificStatesTest
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final RandomRule random = new RandomRule();
    private final TestDirectory directory = TestDirectory.testDirectory( fs );

    @Rule
    public final RuleChain rules = RuleChain.outerRule( random ).around( fs ).around( directory );

    @Test
    public void shouldContinueFromLinkingState() throws Exception
    {
        // given
        crashImportAt( RelationshipLinkbackStage.NAME );

        // when
        importer( new PanicSpreadingExecutionMonitor( RelationshipStage.NAME, true ) ).doImport( input() );

        // then good :)
    }

    @Test
    public void shouldContinueFromCountsState() throws Exception
    {
        // given
        crashImportAt( RelationshipCountsStage.NAME );

        // when
        importer( new PanicSpreadingExecutionMonitor( RelationshipLinkbackStage.NAME, true ) ).doImport( input() );

        // then good :)
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

    private Input input()
    {
        return randomizedInput( random.seed(), 100, 1_000, 0.05f, 0.05f );
    }

    private BatchImporter importer( ExecutionMonitor monitor )
    {
        return new RestartableParallelBatchImporter(
              directory.absolutePath(), fs, null, DEFAULT, NullLogService.getInstance(), monitor,
              EMPTY, Config.defaults(), RecordFormatSelector.defaultFormat() );
    }
}
