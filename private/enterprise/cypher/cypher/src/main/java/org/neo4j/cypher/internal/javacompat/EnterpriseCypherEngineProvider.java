/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.javacompat;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.CommunityCompilerFactory;
import org.neo4j.cypher.internal.CypherConfiguration;
import org.neo4j.cypher.internal.CypherRuntimeConfiguration;
import org.neo4j.cypher.internal.EnterpriseCompilerFactory;
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;

@ServiceProvider
public class EnterpriseCypherEngineProvider extends QueryEngineProvider
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

    @Override
    protected QueryExecutionEngine createEngine( Dependencies deps, GraphDatabaseAPI graphAPI )
    {
        GraphDatabaseCypherService queryService = new GraphDatabaseCypherService( graphAPI );
        deps.satisfyDependency( queryService );

        DependencyResolver resolver = graphAPI.getDependencyResolver();
        LogService logService = resolver.resolveDependency( LogService.class );
        Monitors monitors = resolver.resolveDependency( Monitors.class );
        Config config = resolver.resolveDependency( Config.class );
        boolean isSystemDatabase = graphAPI.databaseLayout().getDatabaseName().startsWith( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        CypherConfiguration cypherConfig = CypherConfiguration.fromConfig( config );
        CypherPlannerConfiguration plannerConfig = cypherConfig.toCypherPlannerConfiguration( config, isSystemDatabase );
        CypherRuntimeConfiguration runtimeConfig = cypherConfig.toCypherRuntimeConfiguration();
        LogProvider logProvider = logService.getInternalLogProvider();
        CommunityCompilerFactory communityCompilerFactory =
                new CommunityCompilerFactory( queryService, monitors, logProvider, plannerConfig, runtimeConfig );

        EnterpriseCompilerFactory compilerFactory =
                new EnterpriseCompilerFactory( communityCompilerFactory, queryService, monitors, logProvider, plannerConfig, runtimeConfig );

        deps.satisfyDependency( compilerFactory );
        if ( isSystemDatabase )
        {
            CommunityCompilerFactory normalCompilerFactory =
                    new CommunityCompilerFactory( queryService, monitors, logProvider, cypherConfig.toCypherPlannerConfiguration( config, false ), runtimeConfig );
            return new SystemExecutionEngine( queryService, logProvider, compilerFactory, normalCompilerFactory );
        }
        else if ( config.get( GraphDatabaseSettings.snapshot_query ) )
        {
            return new SnapshotExecutionEngine( queryService, config, logProvider, compilerFactory );
        }
        else
        {
            return new ExecutionEngine( queryService, logProvider, compilerFactory );
        }
    }
}
