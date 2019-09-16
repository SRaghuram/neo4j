/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.causalclustering.core.CausalClusterConfigurationValidator;
import com.neo4j.server.database.EnterpriseGraphFactory;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GroupSettingValidator;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.NeoServer;
import org.neo4j.server.database.GraphFactory;

public class EnterpriseBootstrapper extends CommunityBootstrapper
{
    @Override
    protected GraphFactory createGraphFactory( Config config )
    {
        return new EnterpriseGraphFactory();
    }

    @Override
    protected NeoServer createNeoServer( GraphFactory graphFactory, Config config, GraphDatabaseDependencies dependencies )
    {
        return new EnterpriseNeoServer( config, graphFactory, dependencies );
    }

    @Override
    protected List<Class<? extends GroupSettingValidator>> configurationValidators()
    {
        List<Class<? extends GroupSettingValidator>> validators = new ArrayList<>( super.configurationValidators() );
        validators.add( CausalClusterConfigurationValidator.class );
        return validators;
    }
}
