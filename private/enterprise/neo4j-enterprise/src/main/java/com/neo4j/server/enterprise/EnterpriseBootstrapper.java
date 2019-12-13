/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.causalclustering.core.CausalClusterConfigurationValidator;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GroupSettingValidator;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.server.CommunityBootstrapper;

import static com.neo4j.server.enterprise.EnterpriseManagementServiceFactory.createManagementService;

public class EnterpriseBootstrapper extends CommunityBootstrapper
{
    @Override
    protected DatabaseManagementService createNeo( Config config, GraphDatabaseDependencies dependencies )
    {
        return createManagementService( config, dependencies );
    }

    @Override
    protected List<Class<? extends GroupSettingValidator>> configurationValidators()
    {
        List<Class<? extends GroupSettingValidator>> validators = new ArrayList<>( super.configurationValidators() );
        validators.add( CausalClusterConfigurationValidator.class );
        return validators;
    }
}
