/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.javacompat;

import org.neo4j.cypher.internal.CompilerFactory;
import org.neo4j.cypher.internal.CypherRuntimeConfiguration;
import org.neo4j.cypher.internal.EnterpriseCompilerFactory;
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration;

public class EnterpriseCypherEngineProvider extends CommunityCypherEngineProvider
{
    @Override
    protected int enginePriority()
    {
        return 1; // Lower means better. The enterprise version will have a lower number
    }

    @Override
    protected CompilerFactory makeCompilerFactory( GraphDatabaseCypherService queryService,
                                                   SPI spi,
                                                   CypherPlannerConfiguration plannerConfig,
                                                   CypherRuntimeConfiguration runtimeConfig )
    {
        return new EnterpriseCompilerFactory( queryService, spi, plannerConfig, runtimeConfig );
    }
}
