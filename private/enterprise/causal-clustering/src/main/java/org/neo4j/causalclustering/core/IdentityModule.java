/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

public class IdentityModule
{
    public static final String CORE_MEMBER_ID_NAME = "core-member-id";

    private MemberId myself;

    IdentityModule( PlatformModule platformModule, File clusterStateDirectory )
    {
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        LogProvider logProvider = platformModule.logging.getInternalLogProvider();

        Log log = logProvider.getLog( getClass() );

        SimpleStorage<MemberId> memberIdStorage = memberIdStorage( clusterStateDirectory, fileSystem, logProvider );

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

    private static SimpleStorage<MemberId> memberIdStorage( File clusterStateDirectory, FileSystemAbstraction fileSystem, LogProvider logProvider )
    {
        return new SimpleFileStorage<>( fileSystem, clusterStateDirectory, CORE_MEMBER_ID_NAME, new MemberId.Marshal(), logProvider );
    }

    static boolean memberIdExists( File clusterStateDirectory, FileSystemAbstraction fileSystem, LogProvider logProvider )
    {
        return memberIdStorage( clusterStateDirectory, fileSystem, logProvider ).exists();
    }

    public MemberId myself()
    {
        return myself;
    }
}
