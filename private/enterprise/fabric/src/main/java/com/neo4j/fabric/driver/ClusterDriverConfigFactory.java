/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import org.neo4j.driver.Config;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.fabric.executor.Location;

public class ClusterDriverConfigFactory implements DriverConfigFactory
{
    @Override
    public Config createConfig( Location.Remote location )
    {
        throw error();
    }

    @Override
    public SecurityPlan createSecurityPlan( Location.Remote location )
    {
        throw error();
    }

    @Override
    public DriverApi getDriverApi( Location.Remote location )
    {
        throw error();
    }

    private RuntimeException error()
    {
        return new IllegalStateException( "Communication within cluster is not supported yet" );
    }
}
