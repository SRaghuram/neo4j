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
package org.neo4j.bolt.runtime;

import java.time.Duration;
import java.util.Objects;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.v1.runtime.StatementProcessorReleaseManager;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.time.SystemNanoClock;

import static java.lang.String.format;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;

public abstract class AbstractTransactionStatementSPIProvider implements TransactionStateMachineSPIProvider
{
    final Duration txAwaitDuration;
    final SystemNanoClock clock;
    final BoltChannel boltChannel;
    final String defaultDatabaseName;
    private final DatabaseManagementService managementService;

    AbstractTransactionStatementSPIProvider( DatabaseManagementService managementService, String defaultDatabaseName, BoltChannel boltChannel,
            Duration awaitDuration, SystemNanoClock clock )
    {
        this.managementService = managementService;
        this.defaultDatabaseName = defaultDatabaseName;
        this.txAwaitDuration = awaitDuration;
        this.clock = clock;
        this.boltChannel = boltChannel;
    }

    protected abstract TransactionStateMachineSPI newTransactionStateMachineSPI( GraphDatabaseFacade activeDatabase,
            StatementProcessorReleaseManager resourceReleaseManger ) throws BoltIOException;

    @Override
    public TransactionStateMachineSPI getTransactionStateMachineSPI( String databaseName, StatementProcessorReleaseManager resourceReleaseManger )
            throws BoltProtocolBreachFatality, BoltIOException
    {
        String selectedDatabaseName = selectDatabaseName( databaseName );
        try
        {
            GraphDatabaseFacade databaseFacade = (GraphDatabaseFacade) managementService.database( selectedDatabaseName );
            ensureAvailable( databaseFacade );
            return newTransactionStateMachineSPI( databaseFacade, resourceReleaseManger );
        }
        catch ( DatabaseNotFoundException e )
        {
            throw new BoltIOException( Status.Database.DatabaseNotFound,
                    format( "Database does not exists. Database name: '%s'.", selectedDatabaseName ) );
        }
    }

    protected String selectDatabaseName( String databaseName ) throws BoltProtocolBreachFatality
    {
        // old versions of protocol does not support passing database name and any name that
        if ( !Objects.equals( databaseName, ABSENT_DB_NAME ) )
        {
            // This bolt version shall NOT provide us a db name.
            throw new BoltProtocolBreachFatality( format( "Database selection by name not supported by Bolt protocol version lower than BoltV4. " +
                    "Please contact your Bolt client author to report this bug in the client code. Requested database name: '%s'.", databaseName ) );
        }
        return defaultDatabaseName;
    }

    private void ensureAvailable( GraphDatabaseFacade database ) throws BoltIOException
    {
        if ( !database.isAvailable( 0 ) )
        {
            throw new BoltIOException( Status.General.DatabaseUnavailable, format( "Database `%s` is unavailable.", database.databaseName() ) );
        }
    }
}
