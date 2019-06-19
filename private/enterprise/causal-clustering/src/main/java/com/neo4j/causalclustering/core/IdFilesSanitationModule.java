/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.IdFilesDeleter;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

/**
 * Removes the ID-files if the instance was unbound. Reason being that an unbound instance likely
 * has a store which has been tampered with in some sense, e.g. copied around, restored from a backup,
 * dumped/loaded etc and we cannot trust that the ID-files have not been copied around. ID-files are
 * private per instance and shall not be copied around.
 *
 * This is thus a defensive strategy to avoid corruption caused by various operational actions.
 */
public class IdFilesSanitationModule extends LifecycleAdapter
{
    private final DatabaseId databaseId;
    private final DatabaseManager<ClusteredDatabaseContext> databaseManager;
    private final FileSystemAbstraction fileSystem;
    private final Log log;
    private final StartupCoreStateCheck startupCoreStateCheck;

    IdFilesSanitationModule( StartupCoreStateCheck startupCoreStateCheck, DatabaseId databaseId, DatabaseManager<ClusteredDatabaseContext> databaseManager,
            FileSystemAbstraction fileSystem, LogProvider logProvider )
    {
        this.startupCoreStateCheck = startupCoreStateCheck;
        this.databaseId = databaseId;
        this.databaseManager = databaseManager;
        this.fileSystem = fileSystem;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start()
    {
        if ( !startupCoreStateCheck.wasUnboundOnStartup() )
        {
            return;
        }

        databaseManager.getDatabaseContext( databaseId ).ifPresent( this::deleteIdFiles );
    }

    private void deleteIdFiles( ClusteredDatabaseContext databaseContext )
    {
        if ( IdFilesDeleter.deleteIdFiles( databaseContext.databaseLayout(), fileSystem ) )
        {
            log.info( format( "ID-files deleted for %s", databaseContext.databaseId().name() ) );
        }
    }
}
