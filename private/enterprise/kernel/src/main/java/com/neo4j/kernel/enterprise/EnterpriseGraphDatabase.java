/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.enterprise;

import com.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;

import java.io.File;

import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class EnterpriseGraphDatabase extends GraphDatabaseFacade
{
    public EnterpriseGraphDatabase( File storeDir, Config config,
            GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        new GraphDatabaseFacadeFactory( DatabaseInfo.COMMERCIAL, EnterpriseEditionModule::new )
                .initFacade( storeDir, config, dependencies, this );
    }
}
