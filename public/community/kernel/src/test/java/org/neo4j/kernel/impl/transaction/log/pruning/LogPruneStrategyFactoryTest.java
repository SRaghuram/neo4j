/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.transaction.log.pruning;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.ThresholdConfigValue;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.time.Clocks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory.getThresholdByType;

class LogPruneStrategyFactoryTest
{

    private FileSystemAbstraction fsa;
    private Clock clock;
    private AssertableLogProvider logProvider;

    @BeforeEach
    void setUp()
    {
        fsa = new DefaultFileSystemAbstraction();
        clock = Clocks.systemClock();
        logProvider = new AssertableLogProvider();
    }

    @Test
    void configuringFilesThreshold()
    {
        Threshold threshold = getThreshold( new ThresholdConfigValue( "files", 25 ) );
        assertThat( threshold, instanceOf( FileCountThreshold.class ) );
        assertEquals( "25 files", threshold.toString() );
    }

    @Test
    void configuringSizeThreshold()
    {
        Threshold threshold = getThreshold( new ThresholdConfigValue( "size", 16000 ) );
        assertThat( threshold, instanceOf( FileSizeThreshold.class ) );
        assertEquals( "16000 size", threshold.toString() );
    }

    @Test
    void configuringTxsThreshold()
    {
        Threshold threshold = getThreshold( new ThresholdConfigValue( "txs", 4000 ) );
        assertThat( threshold, instanceOf( EntryCountThreshold.class ) );
        assertEquals( "4000 entries", threshold.toString() );
    }

    @Test
    void configuringEntriesThreshold()
    {
        Threshold threshold = getThreshold( new ThresholdConfigValue( "entries", 4000 ) );
        assertThat( threshold, instanceOf( EntryCountThreshold.class ) );
        assertEquals( "4000 entries", threshold.toString() );
    }

    @Test
    void configuringHoursThreshold()
    {
        Threshold threshold = getThreshold( new ThresholdConfigValue( "hours", 100 ) );
        assertThat( threshold, instanceOf( EntryTimespanThreshold.class ) );
        assertEquals( "100 hours", threshold.toString() );
    }

    @Test
    void configuringDaysThreshold()
    {
        Threshold threshold = getThreshold( new ThresholdConfigValue( "days", 100_000 ) );
        assertThat( threshold, instanceOf( EntryTimespanThreshold.class ) );
        assertEquals( "100000 days", threshold.toString() );
    }

    private Threshold getThreshold( ThresholdConfigValue configValue )
    {
        return getThresholdByType( fsa, logProvider, clock, configValue, "" );
    }
}
