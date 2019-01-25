/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition;

import java.io.File;

import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class CommercialGraphDatabase extends GraphDatabaseFacade
{
    public CommercialGraphDatabase( File storeDir, Config config, ExternalDependencies dependencies )
    {
        new GraphDatabaseFacadeFactory( DatabaseInfo.COMMERCIAL, CommercialEditionModule::new )
                .initFacade( storeDir, config, dependencies, this );
    }
}
