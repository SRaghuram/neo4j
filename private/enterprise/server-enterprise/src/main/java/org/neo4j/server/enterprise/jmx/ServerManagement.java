/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.enterprise.jmx;

import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.server.NeoServer;

public final class ServerManagement implements ServerManagementMBean
{
    private final NeoServer server;

    public ServerManagement( NeoServer server )
    {
        this.server = server;
    }

    @Override
    public synchronized void restartServer()
    {
        final Log log = server.getDatabase().getGraph().getDependencyResolver().resolveDependency( LogService.class )
                .getUserLog( getClass() );

        Thread thread = new Thread( "Restart server thread" )
        {
            @Override
            public void run()
            {
                log.info( "Restarting server" );
                server.stop();
                server.start();
            }
        };
        thread.setDaemon( false );
        thread.start();

        try
        {
            thread.join();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }
}
