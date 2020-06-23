/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.messaging.marshalling.BooleanMarshal;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import com.neo4j.dbms.EnterpriseOperatorState;
import com.neo4j.dbms.RemoteDatabaseManagementException;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;

import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;

public class DiscoveryDatabaseStateMarshal extends SafeChannelMarshal<DiscoveryDatabaseState>
{
    public static final DiscoveryDatabaseStateMarshal INSTANCE = new DiscoveryDatabaseStateMarshal();

    private DiscoveryDatabaseStateMarshal()
    {
    }

    @Override
    protected DiscoveryDatabaseState unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        var databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
        var operatorState = getOperatorState( channel.getInt() );
        var hasFailed = BooleanMarshal.unmarshal( channel ) ;
        final RemoteDatabaseManagementException failure;

        if ( hasFailed )
        {
            var failureMessage = StringMarshal.unmarshal( channel );
            failure = new RemoteDatabaseManagementException( failureMessage );
        }
        else
        {
            failure = null;
        }
        return new DiscoveryDatabaseState( databaseId, operatorState, failure );
    }

    @Override
    public void marshal( DiscoveryDatabaseState databaseState, WritableChannel channel ) throws IOException
    {
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( databaseState.databaseId(), channel );
        channel.putInt( databaseState.operatorState().ordinal() );
        BooleanMarshal.marshal( channel, databaseState.hasFailed() );
        var failure = databaseState.failure();
        if ( failure.isPresent() )
        {
            StringMarshal.marshal( channel, failure.get().getMessage() );
        }

    }

    private EnterpriseOperatorState getOperatorState( int ordinal )
    {
        var operatorStates = EnterpriseOperatorState.values();
        if ( ordinal < 0 || ordinal >= operatorStates.length )
        {
            return UNKNOWN;
        }
        return operatorStates[ordinal];
    }
}
