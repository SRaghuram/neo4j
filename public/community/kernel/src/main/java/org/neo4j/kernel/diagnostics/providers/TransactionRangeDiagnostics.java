/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.diagnostics.providers;

import java.io.IOException;

import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.Logger;

public class TransactionRangeDiagnostics extends NamedDiagnosticsProvider
{
    private final NeoStoreDataSource dataSource;

    TransactionRangeDiagnostics( NeoStoreDataSource dataSource )
    {
        super( "Transaction log" );
        this.dataSource = dataSource;
    }

    @Override
    public void dump( Logger logger )
    {
        LogFiles logFiles = dataSource.getDependencyResolver().resolveDependency( LogFiles.class );
        try
        {
            for ( long logVersion = logFiles.getLowestLogVersion(); logFiles.versionExists( logVersion ); logVersion++ )
            {
                if ( logFiles.hasAnyEntries( logVersion ) )
                {
                    LogHeader header = logFiles.extractHeader( logVersion );
                    long firstTransactionIdInThisLog = header.lastCommittedTxId + 1;
                    logger.log( "Oldest transaction " + firstTransactionIdInThisLog + " found in log with version " + logVersion );
                    return;
                }
            }
            logger.log( "No transactions found in any log" );
        }
        catch ( IOException e )
        {
            logger.log( "Error trying to figure out oldest transaction in log" );
        }
    }
}
