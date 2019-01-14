/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.com.slave;

import org.neo4j.com.ProtocolVersion;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.Server;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlaveClient.SlaveRequestType;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

import static org.neo4j.com.Protocol.DEFAULT_FRAME_LENGTH;
import static org.neo4j.com.ProtocolVersion.INTERNAL_PROTOCOL_VERSION;
import static org.neo4j.com.TxChecksumVerifier.ALWAYS_MATCH;

public class SlaveServer extends Server<Slave, Void>
{
    public static final byte APPLICATION_PROTOCOL_VERSION = 1;
    public static final ProtocolVersion SLAVE_PROTOCOL_VERSION =
            new ProtocolVersion( (byte) 1, INTERNAL_PROTOCOL_VERSION );

    public SlaveServer( Slave requestTarget, Configuration config, LogProvider logProvider,
            ByteCounterMonitor byteCounterMonitor, RequestMonitor requestMonitor )
    {
        super( requestTarget, config, logProvider, DEFAULT_FRAME_LENGTH, SLAVE_PROTOCOL_VERSION, ALWAYS_MATCH,
                Clocks.systemClock(), byteCounterMonitor, requestMonitor );
    }

    @Override
    protected RequestType getRequestContext( byte id )
    {
        return SlaveRequestType.values()[id];
    }

    @Override
    protected void stopConversation( RequestContext context )
    {
    }
}
