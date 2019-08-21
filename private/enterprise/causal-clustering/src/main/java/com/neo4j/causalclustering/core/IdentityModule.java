/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.logging.Log;

public class IdentityModule
{
    private MemberId myself;

    IdentityModule( GlobalModule globalModule, ClusterStateStorageFactory storageFactory )
    {
        Log log = globalModule.getLogService().getInternalLogProvider().getLog( getClass() );

        SimpleStorage<MemberId> memberIdStorage = storageFactory.createMemberIdStorage();

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

        globalModule.getJobScheduler().setTopLevelGroupName( "Core " + myself );
    }

    public MemberId myself()
    {
        return myself;
    }
}
