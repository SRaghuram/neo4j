/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.common.IdFilesDeleter;

import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
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
    private final ClusteredDatabaseManager<CoreDatabaseContext> databaseManager;
    private final FileSystemAbstraction fileSystem;
    private final Log log;
    private final StartupCoreStateCheck startupCoreStateCheck;

    IdFilesSanitationModule( StartupCoreStateCheck startupCoreStateCheck, ClusteredDatabaseManager<CoreDatabaseContext> databaseManager,
            FileSystemAbstraction fileSystem, LogProvider logProvider )
    {
        this.startupCoreStateCheck = startupCoreStateCheck;
        this.databaseManager = databaseManager;
        this.fileSystem = fileSystem;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Exception
    {
        if ( !startupCoreStateCheck.wasUnboundOnStartup() )
        {
            return;
        }

        for ( Map.Entry<String,CoreDatabaseContext> dbEntry : databaseManager.registeredDatabases().entrySet() )
        {
            if ( IdFilesDeleter.deleteIdFiles( dbEntry.getValue().databaseLayout(), fileSystem ) )
            {
                log.info( format( "ID-files deleted for %s", dbEntry.getKey() ) );
            }
        }
    }
}
