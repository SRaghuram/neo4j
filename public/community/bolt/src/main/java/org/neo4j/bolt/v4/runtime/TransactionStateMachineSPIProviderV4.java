/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.v4.runtime;

import java.util.Objects;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.runtime.statemachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.impl.AbstractTransactionStatementSPIProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;

public class TransactionStateMachineSPIProviderV4 extends AbstractTransactionStatementSPIProvider
{
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( TransactionStateMachineSPIProviderV4.class );

    public TransactionStateMachineSPIProviderV4( BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI,
                                                 BoltChannel boltChannel, SystemNanoClock clock, MemoryTracker memoryTracker )
    {
        super( boltGraphDatabaseManagementServiceSPI, boltChannel, clock, memoryTracker );
    }

    @Override
    protected String selectDatabaseName( String databaseName )
    {
        return Objects.equals( databaseName, ABSENT_DB_NAME ) ? boltChannel.defaultDatabase() : databaseName;
    }

    @Override
    protected TransactionStateMachineSPI newTransactionStateMachineSPI( BoltGraphDatabaseServiceSPI activeBoltGraphDatabaseServiceSPI,
            StatementProcessorReleaseManager resourceReleaseManger )
    {
        memoryTracker.allocateHeap( TransactionStateMachineV4SPI.SHALLOW_SIZE );
        return new TransactionStateMachineV4SPI( activeBoltGraphDatabaseServiceSPI, boltChannel, clock, resourceReleaseManger );
    }
}
