/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.test.server.ha;

import java.io.File;
import java.io.IOException;

import org.neo4j.server.enterprise.OpenEnterpriseNeoServer;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;

public class EnterpriseServerHelper
{
    private EnterpriseServerHelper()
    {
    }

    public static OpenEnterpriseNeoServer createNonPersistentServer( File databaseDir ) throws IOException
    {
        return createServer( databaseDir, false );
    }

    private static OpenEnterpriseNeoServer createServer( File databaseDir, boolean persistent ) throws IOException
    {
        EnterpriseServerBuilder builder = EnterpriseServerBuilder.serverOnRandomPorts().usingDataDir( databaseDir.getAbsolutePath() );
        if ( persistent )
        {
            builder = (EnterpriseServerBuilder) builder.persistent();
        }
        builder.withDefaultDatabaseTuning();
        OpenEnterpriseNeoServer server = builder.build();
        server.start();
        return server;
    }
}
