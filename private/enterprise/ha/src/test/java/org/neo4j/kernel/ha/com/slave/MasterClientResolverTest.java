/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.com.slave;

import org.junit.Test;

import org.neo4j.com.IllegalProtocolVersionException;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.function.Suppliers;
import org.neo4j.kernel.ha.MasterClient214;
import org.neo4j.kernel.ha.MasterClient310;
import org.neo4j.kernel.ha.MasterClient320;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreId;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class MasterClientResolverTest
{
    @Test
    public void shouldResolveMasterClientFactory()
    {
        // Given
        LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        MasterClientResolver resolver = new MasterClientResolver( NullLogProvider.getInstance(),
                ResponseUnpacker.NO_OP_RESPONSE_UNPACKER, mock( InvalidEpochExceptionHandler.class ), 1, 1, 1, 1024,
                Suppliers.singleton( logEntryReader ) );

        LifeSupport life = new LifeSupport();
        try
        {
            life.start();
            MasterClient masterClient1 =
                    resolver.instantiate( "cluster://localhost", 44, null, new Monitors(), StoreId.DEFAULT, life );
            assertThat( masterClient1, instanceOf( MasterClient320.class ) );
        }
        finally
        {
            life.shutdown();
        }

        IllegalProtocolVersionException illegalProtocolVersionException = new IllegalProtocolVersionException(
                MasterClient214.PROTOCOL_VERSION.getApplicationProtocol(),
                MasterClient310.PROTOCOL_VERSION.getApplicationProtocol(),
                "Protocol is too modern" );

        // When
        resolver.handle( illegalProtocolVersionException );

        // Then
        life = new LifeSupport();
        try
        {
            life.start();
            MasterClient masterClient2 =
                    resolver.instantiate( "cluster://localhost", 55, null, new Monitors(), StoreId.DEFAULT, life );

            assertThat( masterClient2, instanceOf( MasterClient214.class ) );
        }
        finally
        {
            life.shutdown();
        }
    }
}
