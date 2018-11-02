/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.factory.HighlyAvailableEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

/**
 * This has all the functionality of an Enterprise Edition embedded database, with the addition of services
 * for handling clustering.
 */
public class HighlyAvailableGraphDatabase extends GraphDatabaseFacade
{

    protected HighlyAvailableEditionModule module;

    public HighlyAvailableGraphDatabase( File storeDir, Map<String,String> params,
            GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        newHighlyAvailableFacadeFactory().initFacade( storeDir, params, dependencies, this );
    }

    public HighlyAvailableGraphDatabase( File storeDir, Config config,
            GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        newHighlyAvailableFacadeFactory().initFacade( storeDir, config, dependencies, this );
    }

    // used for testing in a different project, please do not remove
    protected GraphDatabaseFacadeFactory newHighlyAvailableFacadeFactory()
    {
        return new GraphDatabaseFacadeFactory( DatabaseInfo.HA, platformModule ->
        {
            module = new HighlyAvailableEditionModule( platformModule );
            return module;
        } );
    }

    public HighAvailabilityMemberState getInstanceState()
    {
        return module.memberStateMachine.getCurrentState();
    }

    public String role()
    {
        return module.members.getCurrentMemberRole();
    }

    public boolean isMaster()
    {
        return HighAvailabilityModeSwitcher.MASTER.equalsIgnoreCase( role() );
    }
}
