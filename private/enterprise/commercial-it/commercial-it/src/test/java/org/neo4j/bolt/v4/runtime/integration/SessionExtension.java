/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.bolt.v4.runtime.integration;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.BoltStateMachineFactoryImpl;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.udc.UsageData;

public class SessionExtension implements BeforeEachCallback, AfterEachCallback
{
    private GraphDatabaseAPI gdb;
    private BoltStateMachineFactoryImpl boltFactory;
    private List<BoltStateMachine> runningMachines = new ArrayList<>();
    private boolean authEnabled;

    private Authentication authentication( AuthManager authManager, UserManagerSupplier userManagerSupplier )
    {
        return new BasicAuthentication( authManager, userManagerSupplier );
    }

    public BoltStateMachine newMachine( long version, BoltChannel boltChannel )
    {
        assertTestStarted();
        BoltStateMachine machine = boltFactory.newStateMachine( version, boltChannel );
        runningMachines.add( machine );
        return machine;
    }

    public DatabaseManager databaseManager()
    {
        assertTestStarted();
        DependencyResolver resolver = gdb.getDependencyResolver();
        return resolver.resolveDependency( DatabaseManager.class );
    }

    public String defaultDatabaseName()
    {
        assertTestStarted();
        DependencyResolver resolver = gdb.getDependencyResolver();
        Config config = resolver.resolveDependency( Config.class );
        return config.get( GraphDatabaseSettings.default_database );
    }

    private void assertTestStarted()
    {
        if ( boltFactory == null || gdb == null )
        {
            throw new IllegalStateException( "Cannot access test environment before test is running." );
        }
    }

    @Override
    public void beforeEach( ExtensionContext extensionContext )
    {
        Map<Setting<?>,String> configMap = new HashMap<>();
        configMap.put( GraphDatabaseSettings.auth_enabled, Boolean.toString( authEnabled ) );
        gdb = (GraphDatabaseAPI) new TestCommercialGraphDatabaseFactory().newImpermanentDatabase( configMap );
        DependencyResolver resolver = gdb.getDependencyResolver();
        Config config = resolver.resolveDependency( Config.class );
        Authentication authentication = authentication( resolver.resolveDependency( AuthManager.class ),
                resolver.resolveDependency( UserManagerSupplier.class ) );
        boltFactory = new BoltStateMachineFactoryImpl(
                resolver.resolveDependency( DatabaseManager.class ),
                new UsageData( null ),
                authentication,
                Clock.systemUTC(),
                config,
                NullLogService.getInstance()
        );
    }

    @Override
    public void afterEach( ExtensionContext extensionContext )
    {
        try
        {
            if ( runningMachines != null )
            {
                IOUtils.closeAll( runningMachines );
            }
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
        }

        gdb.shutdown();
    }
}
