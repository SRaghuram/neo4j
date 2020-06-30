/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.enterprise.edition.EnterpriseEditionModule;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.mockito.MockingDetails;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.SystemNanoClock;

public class TestFabricDatabaseManagementServiceBuilder extends TestEnterpriseDatabaseManagementServiceBuilder
{

    private Dependencies dependencies;
    private final List<Object> mocks;

    public TestFabricDatabaseManagementServiceBuilder( Path databaseRootDir, List<Object> mocks )
    {
        super( databaseRootDir );
        this.mocks = mocks;
    }

    @Override
    protected Function<GlobalModule,AbstractEditionModule> getEditionFactory( Config config )
    {
        return globalModule -> {
            dependencies = createDependencyResolver( globalModule.getGlobalDependencies() );
            return new EnterpriseEditionModule( globalModule, dependencies )
            {
                @Override
                public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider( Dependencies dependenciesWithoutMocks,
                        DatabaseManagementService managementService, Monitors monitors, SystemNanoClock clock, LogService logService )
                {
                    return (BoltGraphDatabaseManagementServiceSPI) mocks.stream()
                            .filter( mock -> mock instanceof BoltGraphDatabaseManagementServiceSPI )
                            .findAny()
                            .orElseGet( () -> super.createBoltDatabaseManagementServiceProvider( dependencies,
                                    managementService,
                                    monitors,
                                    clock,
                                    logService ) );
                }
            };
        };
    }

    @Override
    public String getEdition()
    {
        return "Fabric";
    }

    public Dependencies getDependencies()
    {
        return dependencies;
    }

    private Dependencies createDependencyResolver( Dependencies parent )
    {
        return new Dependencies( parent )
        {
            {
                mocks.forEach( super::satisfyDependency );
            }

            @Override
            public <T> T resolveDependency( Class<T> type ) throws IllegalArgumentException
            {
                return super.resolveDependency( type, new MockPreferringSelectionStrategy() );
            }

            @Override
            public <T> T satisfyDependency( T dependency )
            {
                return parent.satisfyDependency( dependency );
            }
        };
    }

    private static class MockPreferringSelectionStrategy implements DependencyResolver.SelectionStrategy
    {

        @Override
        public <T> T select( Class<T> type, Iterable<? extends T> candidates ) throws IllegalArgumentException
        {
            Iterator<? extends T> iterator = candidates.iterator();
            if ( !iterator.hasNext() )
            {
                throw new IllegalArgumentException( "Could not resolve dependency of type:" + type.getName() );
            }

            T first = iterator.next();
            if ( !iterator.hasNext() )
            {
                return first;
            }

            T second = iterator.next();

            MockingDetails firstMockingDetails = Mockito.mockingDetails( first );
            MockingDetails secondMockingDetails = Mockito.mockingDetails( second );
            if ( iterator.hasNext() || firstMockingDetails.isMock() && secondMockingDetails.isMock() ||
                    !firstMockingDetails.isMock() && !secondMockingDetails.isMock() )
            {
                throw new IllegalArgumentException( "Multiple dependencies of type:" + type.getName() );
            }

            if ( firstMockingDetails.isMock() )
            {
                return first;
            }

            return second;
        }
    }
}
