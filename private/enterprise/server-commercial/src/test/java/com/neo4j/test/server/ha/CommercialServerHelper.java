/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.server.ha;

import com.neo4j.server.enterprise.CommercialNeoServer;
import com.neo4j.server.enterprise.helpers.CommercialServerBuilder;

import java.io.File;
import java.io.IOException;

public class CommercialServerHelper
{
    private CommercialServerHelper()
    {
    }

    public static CommercialNeoServer createNonPersistentServer( File databaseDir ) throws IOException
    {
        return createServer( databaseDir, false );
    }

    private static CommercialNeoServer createServer( File databaseDir, boolean persistent ) throws IOException
    {
        CommercialServerBuilder builder = CommercialServerBuilder.serverOnRandomPorts().usingDataDir( databaseDir.getAbsolutePath() );
        if ( persistent )
        {
            builder = (CommercialServerBuilder) builder.persistent();
        }
        builder.withDefaultDatabaseTuning();
        CommercialNeoServer server = builder.build();
        server.start();
        return server;
    }
}
