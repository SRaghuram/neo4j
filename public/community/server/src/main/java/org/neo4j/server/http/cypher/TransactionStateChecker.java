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
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

public class TransactionStateChecker implements AutoCloseable
{
    private final Statement statement;
    private final IsNodeDeletedInCurrentTx nodeCheck;
    private final IsRelationshipDeletedInCurrentTx relCheck;

    public TransactionStateChecker( Statement statement, IsNodeDeletedInCurrentTx nodeCheck,
            IsRelationshipDeletedInCurrentTx relCheck )
    {
        this.statement = statement;
        this.nodeCheck = nodeCheck;
        this.relCheck = relCheck;
    }

    public static TransactionStateChecker create( TransitionalPeriodTransactionMessContainer container )
    {
        ThreadToStatementContextBridge txBridge = container.getBridge();
        KernelTransaction topLevelTransactionBoundToThisThread =
                txBridge.getKernelTransactionBoundToThisThread( true, container.getDb().databaseId() );
        KernelStatement kernelStatement = (KernelStatement) topLevelTransactionBoundToThisThread.acquireStatement();

        return new TransactionStateChecker(
                kernelStatement,
                nodeId -> kernelStatement.hasTxStateWithChanges() &&
                          kernelStatement.txState().nodeIsDeletedInThisTx( nodeId ),
                relId -> kernelStatement.hasTxStateWithChanges() &&
                         kernelStatement.txState().relationshipIsDeletedInThisTx( relId ) );
    }

    @Override
    public void close()
    {
        statement.close();
    }

    public boolean isNodeDeletedInCurrentTx( long id )
    {
        return nodeCheck.test( id );
    }

    public boolean isRelationshipDeletedInCurrentTx( long id )
    {
        return relCheck.test( id );
    }

    @FunctionalInterface
    public interface IsNodeDeletedInCurrentTx
    {
        boolean test( long id );
    }

    @FunctionalInterface
    public interface IsRelationshipDeletedInCurrentTx
    {
        boolean test( long id );
    }
}
