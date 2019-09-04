/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.causalclustering.core.state.ClusterStateCleaner;
import org.neo4j.causalclustering.core.state.CoreStateStorageService;
import org.neo4j.causalclustering.core.state.storage.SimpleStorage;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import static org.neo4j.causalclustering.core.state.CoreStateFiles.CORE_MEMBER_ID;

public class MemberIdRepository extends LifecycleAdapter
{
    private final MemberId myself;
    private final SimpleStorage<MemberId> memberIdStorage;
    private final boolean replaceExistingState;

    public MemberIdRepository( PlatformModule platformModule, CoreStateStorageService storage, ClusterStateCleaner clusterStateCleaner )
    {
        Log log = platformModule.logging.getInternalLogProvider().getLog( getClass() );

        memberIdStorage = storage.simpleStorage( CORE_MEMBER_ID );

        try
        {
            if ( memberIdStorage.exists() && !clusterStateCleaner.stateUnclean() )
            {
                myself = memberIdStorage.readState();
                replaceExistingState = false;
                if ( myself == null )
                {
                    throw new RuntimeException( "MemberId stored on disk was null" );
                }
            }
            else
            {
                UUID uuid = UUID.randomUUID();
                myself = new MemberId( uuid );
                replaceExistingState = true;
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

    @Override
    public void init() throws Throwable
    {
       if ( replaceExistingState )
       {
           memberIdStorage.writeState( myself );
       }
    }
}
