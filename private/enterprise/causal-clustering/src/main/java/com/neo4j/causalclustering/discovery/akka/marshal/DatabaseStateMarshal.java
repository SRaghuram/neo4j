/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.BooleanMarshal;
import com.neo4j.causalclustering.messaging.marshalling.DatabaseIdMarshal;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import com.neo4j.dbms.EnterpriseDatabaseState;
import com.neo4j.dbms.EnterpriseOperatorState;
import com.neo4j.dbms.RemoteDatabaseManagementException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.OperatorState;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;

public class DatabaseStateMarshal extends SafeChannelMarshal<DatabaseState>
{
    public static DatabaseStateMarshal INSTANCE = new DatabaseStateMarshal();
    private static int unknownStateOrdinal = getUnknownStateOrdinal();

    private DatabaseStateMarshal()
    {
    }

    @Override
    protected DatabaseState unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        var databaseId = DatabaseIdMarshal.INSTANCE.unmarshal( channel );
        var operatorState = getOperatorState( channel.getInt() );
        var hasFailed = BooleanMarshal.unmarshal( channel ) ;
        var state = new EnterpriseDatabaseState( databaseId, operatorState );

        if ( hasFailed )
        {
            var failureMessage = StringMarshal.unmarshal( channel );
            return state.failed( new RemoteDatabaseManagementException( failureMessage ) );
        }
        return state;
    }

    @Override
    public void marshal( DatabaseState databaseState, WritableChannel channel ) throws IOException
    {
        DatabaseIdMarshal.INSTANCE.marshal( databaseState.databaseId(), channel );
        channel.putInt( databaseState.operatorState().ordinal() );
        BooleanMarshal.marshal( channel, databaseState.hasFailed() );
        var failure = databaseState.failure();
        if ( failure.isPresent() )
        {
            StringMarshal.marshal( channel, failure.get().getMessage() );
        }

    }

    private static int getUnknownStateOrdinal()
    {
        var operatorStates = Arrays.asList( EnterpriseOperatorState.values() );
        return operatorStates.indexOf( UNKNOWN );
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
