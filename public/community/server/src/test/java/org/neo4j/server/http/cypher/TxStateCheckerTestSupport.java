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
package org.neo4j.server.http.cypher;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TxStateCheckerTestSupport
{
    public static final TransitionalPeriodTransactionMessContainer CONTAINER = mock( TransitionalPeriodTransactionMessContainer.class );
    private static final FakeBridge FAKE_BRIDGE = new FakeBridge();

    static
    {
        when( CONTAINER.getBridge() ).thenReturn( FAKE_BRIDGE );
        GraphDatabaseFacade facade = mock( GraphDatabaseFacade.class );
        when( CONTAINER.getDb() ).thenReturn( facade );
        when( facade.databaseId() ).thenReturn( TestDatabaseIdRepository.randomDatabaseId() );
    }

    public static class FakeBridge extends ThreadToStatementContextBridge
    {
        private final KernelTransaction tx = mock( KernelTransaction.class );
        private final KernelStatement statement = mock( KernelStatement.class );

        FakeBridge()
        {
            when( tx.acquireStatement() ).thenReturn( statement );
            when( statement.hasTxStateWithChanges() ).thenReturn( false );
        }

        @Override
        public KernelTransaction getKernelTransactionBoundToThisThread( boolean strict, DatabaseId databaseId )
        {
            return tx;
        }
    }
}
