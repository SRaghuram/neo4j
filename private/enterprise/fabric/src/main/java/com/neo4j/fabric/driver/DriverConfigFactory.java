/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import org.neo4j.configuration.GraphDatabaseSettings.DriverApi;
import org.neo4j.driver.Config;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.fabric.executor.Location;

public interface DriverConfigFactory
{
    Config createConfig( Location.Remote location );

    SecurityPlan createSecurityPlan( Location.Remote location );

    DriverApi getDriverApi( Location.Remote location );
}
