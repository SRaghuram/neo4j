/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.state.CoreStateStorageService;

import static com.neo4j.causalclustering.core.state.CoreStateFiles.CORE_MEMBER_ID;

/**
 * This class semantically captures the cluster state as it was on startup. Because the
 * check is performed on startup in the constructor, it is important to construct this
 * class before the cluster-state is touched by other modules.
 */
class CoreStartupState
{
    private final boolean wasUnboundOnStartup;

    CoreStartupState( CoreStateStorageService storage )
    {
        /* This check is extremely simple and only considers a single file of the cluster state. It is
           good enough, but it could be improved together with more strict handling of cluster-state. */

        wasUnboundOnStartup = !storage.simpleStorage( CORE_MEMBER_ID ).exists();
    }

    boolean wasUnboundOnStartup()
    {
        return wasUnboundOnStartup;
    }
}
