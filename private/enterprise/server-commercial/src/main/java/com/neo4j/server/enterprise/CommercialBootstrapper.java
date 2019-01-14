/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.server.database.CommercialGraphFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.causalclustering.core.CausalClusterConfigurationValidator;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationValidator;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.NeoServer;
import org.neo4j.server.database.GraphFactory;

public class CommercialBootstrapper extends CommunityBootstrapper
{
    @Override
    protected GraphFactory createGraphFactory( Config config )
    {
        return new CommercialGraphFactory();
    }

    @Override
    protected NeoServer createNeoServer( GraphFactory graphFactory, Config config, GraphDatabaseDependencies dependencies )
    {
        return new CommercialNeoServer( config, graphFactory, dependencies );
    }

    @Override
    protected Collection<ConfigurationValidator> configurationValidators()
    {
        List<ConfigurationValidator> validators = new ArrayList<>( super.configurationValidators() );
        validators.add( new CausalClusterConfigurationValidator() );
        return validators;
    }
}
