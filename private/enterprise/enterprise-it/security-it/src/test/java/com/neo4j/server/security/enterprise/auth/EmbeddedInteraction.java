/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

public class EmbeddedInteraction implements NeoInteractionLevel<EnterpriseLoginContext>
{
    private GraphDatabaseFacade db;
    private EnterpriseAuthManager authManager;
    private DatabaseManagementService managementService;

    EmbeddedInteraction( Map<Setting<?>, String> config, TestDirectory testDirectory )
    {
        DatabaseManagementServiceBuilder builder = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .impermanent();
        init( builder, config );
    }

    @SuppressWarnings( "unchecked" )
    private void init( DatabaseManagementServiceBuilder builder, Map<Setting<?>,String> config )
    {
        builder.setConfig( BoltConnector.enabled, true );
        builder.setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) );
        builder.setConfig( GraphDatabaseSettings.auth_enabled, true );

        config.forEach( ( setting, valueStr ) ->
        {
            var settingObj = (SettingImpl<Object>) setting;
            builder.setConfig( settingObj, settingObj.parse( valueStr ) );
        } );

        managementService = builder.build();
        db = (GraphDatabaseFacade) managementService.database( DEFAULT_DATABASE_NAME );
        authManager = db.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
    }

    @Override
    public GraphDatabaseFacade getLocalGraph()
    {
        return db;
    }

    @Override
    public GraphDatabaseFacade getSystemGraph()
    {
        return (GraphDatabaseFacade) managementService.database( SYSTEM_DATABASE_NAME );
    }

    @Override
    public void shutdown()
    {
        managementService.shutdown();
    }

    @Override
    public FileSystemAbstraction fileSystem()
    {
        return db.getDependencyResolver().resolveDependency( FileSystemAbstraction.class );
    }

    @Override
    public InternalTransaction beginLocalTransactionAsUser( EnterpriseLoginContext loginContext, KernelTransaction.Type txType )
    {
        return db.beginTransaction( txType, loginContext );
    }

    @Override
    public InternalTransaction beginLocalTransactionAsUser( EnterpriseLoginContext loginContext, KernelTransaction.Type txType, String database )
    {
        var gdb = (GraphDatabaseFacade) managementService.database( database );
        return gdb.beginTransaction( txType, loginContext );
    }

    @Override
    public String executeQuery( EnterpriseLoginContext loginContext, String database, String call, Map<String,Object> params,
                                Consumer<ResourceIterator<Map<String,Object>>> resultConsumer )
    {
        var gdb = (GraphDatabaseFacade) managementService.database( database );
        try ( InternalTransaction tx = gdb.beginTransaction( KernelTransaction.Type.IMPLICIT, loginContext ) )
        {
            Map<String,Object> p = (params == null) ? Collections.emptyMap() : params;
            resultConsumer.accept( tx.execute( call, p ) );
            tx.commit();
            return "";
        }
        catch ( Exception e )
        {
            return e.getMessage();
        }
    }

    @Override
    public EnterpriseLoginContext login( String username, String password ) throws Exception
    {
        return authManager.login( authToken( username, password ) );
    }

    @Override
    public void logout( EnterpriseLoginContext loginContext )
    {
        loginContext.subject().logout();
    }

    @Override
    public void updateAuthToken( EnterpriseLoginContext subject, String username, String password )
    {
    }

    @Override
    public String nameOf( EnterpriseLoginContext loginContext )
    {
        return loginContext.subject().username();
    }

    @Override
    public void tearDown()
    {
        managementService.shutdown();
    }

    @Override
    public void assertAuthenticated( EnterpriseLoginContext loginContext )
    {
        assertThat( loginContext.subject().getAuthenticationResult(), equalTo( AuthenticationResult.SUCCESS ) );
    }

    @Override
    public void assertPasswordChangeRequired( EnterpriseLoginContext loginContext )
    {
        assertThat( loginContext.subject().getAuthenticationResult(), equalTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) );
    }

    @Override
    public void assertUnauthenticated( EnterpriseLoginContext loginContext )
    {
        assertThat( loginContext.subject().getAuthenticationResult(), equalTo( AuthenticationResult.FAILURE ) );
    }

    @Override
    public String getConnectionProtocol()
    {
        return "embedded";
    }

    @Override
    public void registerTransactionEventListener( String databaseName, TransactionEventListener<?> listener )
    {
        managementService.registerTransactionEventListener( databaseName, listener );
    }

    @Override
    public void unregisterTransactionEventListener( String databaseName, TransactionEventListener<?> listener )
    {
        managementService.unregisterTransactionEventListener( databaseName, listener );
    }
}
