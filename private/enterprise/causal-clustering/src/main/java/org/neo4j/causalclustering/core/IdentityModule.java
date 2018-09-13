/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.neo4j.causalclustering.core.state.storage.SimpleFileStorage;
import org.neo4j.causalclustering.core.state.storage.SimpleStorage;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.causalclustering.core.state.CoreStateFiles.CORE_MEMBER_ID;

public class IdentityModule
{
    private MemberId myself;

    IdentityModule( PlatformModule platformModule, File clusterStateDirectory )
    {
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        LogProvider logProvider = platformModule.logging.getInternalLogProvider();

        Log log = logProvider.getLog( getClass() );

        SimpleStorage<MemberId> memberIdStorage = new SimpleFileStorage<>( fileSystem, CORE_MEMBER_ID.at( clusterStateDirectory ),
                new MemberId.Marshal(), logProvider );

        try
        {
            if ( memberIdStorage.exists() )
            {
                myself = memberIdStorage.readState();
                if ( myself == null )
                {
                    throw new RuntimeException( "I was null" );
                }
            }
            else
            {
                UUID uuid = UUID.randomUUID();
                myself = new MemberId( uuid );
                memberIdStorage.writeState( myself );

                log.info( String.format( "Generated new id: %s (%s)", myself, uuid ) );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        platformModule.jobScheduler.setTopLevelGroupName( "Core " + myself );
    }

    public MemberId myself()
    {
        return myself;
    }
}
