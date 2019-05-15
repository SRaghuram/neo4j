/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.javacompat;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.cypher.internal.CompilerFactory;
import org.neo4j.cypher.internal.CypherRuntimeConfiguration;
import org.neo4j.cypher.internal.EnterpriseCompilerFactory;
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration;

@ServiceProvider
public class EnterpriseCypherEngineProvider extends CommunityCypherEngineProvider
{
    @Override
    public String getName()
    {
        return "enterprise-cypher";
    }

    @Override
    protected int enginePriority()
    {
        return 1; // Lower means better. The enterprise version will have a lower number
    }

    protected CompilerFactory makeCompilerFactory( GraphDatabaseCypherService queryService,
                                                   SPI spi,
                                                   CypherPlannerConfiguration plannerConfig,
                                                   CypherRuntimeConfiguration runtimeConfig )
    {
        return new EnterpriseCompilerFactory( queryService, spi, plannerConfig, runtimeConfig );
    }
}
