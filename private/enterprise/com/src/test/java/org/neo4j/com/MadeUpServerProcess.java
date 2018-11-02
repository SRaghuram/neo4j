/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import java.util.concurrent.TimeUnit;

import org.neo4j.test.subprocess.SubProcess;

import static org.neo4j.com.StoreIdTestFactory.newStoreIdForCurrentVersion;

public class MadeUpServerProcess extends SubProcess<ServerInterface, StartupData> implements ServerInterface
{
    private static final long serialVersionUID = 1L;

    private transient volatile MadeUpServer server;

    @Override
    protected void startup( StartupData data ) throws Throwable
    {
        MadeUpCommunicationInterface implementation = new MadeUpServerImplementation(
                newStoreIdForCurrentVersion( data.creationTime, data.storeId, data.creationTime, data.storeId ) );
        MadeUpServer localServer = new MadeUpServer( implementation, data.port, data.internalProtocolVersion,
                data.applicationProtocolVersion, TxChecksumVerifier.ALWAYS_MATCH, data.chunkSize );
        localServer.init();
        localServer.start();
        // The field being non null is an indication of startup, so assign last
        server = localServer;
    }

    @Override
    public void awaitStarted()
    {
        try
        {
            long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( 60 );
            while ( server == null && System.currentTimeMillis() < endTime )
            {
                Thread.sleep( 10 );
            }
            if ( server == null )
            {
                throw new RuntimeException( "Couldn't start server, wait timeout" );
            }
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    protected void shutdown( boolean normal )
    {
        if ( server != null )
        {
            try
            {
                server.stop();
                server.shutdown();
            }
            catch ( Throwable throwable )
            {
                throw new RuntimeException( throwable );
            }
        }
        new Thread( () -> {
            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
            shutdownProcess();
        } ).start();
    }

    protected void shutdownProcess()
    {
        super.shutdown();
    }
}
