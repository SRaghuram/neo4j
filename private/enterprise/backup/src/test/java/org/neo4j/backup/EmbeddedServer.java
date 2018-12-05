/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.TestGraphDatabaseFactory;

public class EmbeddedServer implements ServerInterface
{
    private GraphDatabaseService db;

    public EmbeddedServer( File storeDir, String serverAddress )
    {
        GraphDatabaseBuilder graphDatabaseBuilder = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir );
        graphDatabaseBuilder.setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE );
        graphDatabaseBuilder.setConfig( OnlineBackupSettings.online_backup_server, serverAddress );
        graphDatabaseBuilder.setConfig( GraphDatabaseSettings.pagecache_memory, "8m" );
        this.db = graphDatabaseBuilder.newGraphDatabase();
    }

    @Override
    public void shutdown()
    {
        db.shutdown();
    }

    @Override
    public void awaitStarted()
    {
    }
}
