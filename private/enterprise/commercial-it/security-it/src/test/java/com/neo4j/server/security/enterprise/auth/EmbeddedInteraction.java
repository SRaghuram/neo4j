/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.CommercialAuthManager;
import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
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
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

public class EmbeddedInteraction implements NeoInteractionLevel<CommercialLoginContext>
{
    private GraphDatabaseFacade db;
    private GraphDatabaseFacade systemDB;
    private CommercialAuthManager authManager;
    private ConnectorPortRegister connectorRegister;
    private DatabaseManagementService managementService;

    EmbeddedInteraction( Map<Setting<?>, String> config, TestDirectory testDirectory ) throws Throwable
    {
        DatabaseManagementServiceBuilder builder = new TestCommercialDatabaseManagementServiceBuilder( testDirectory.storeDir() );
        init( builder, config );
    }

    private void init( DatabaseManagementServiceBuilder builder, Map<Setting<?>,String> config ) throws Throwable
    {
        builder.setConfig( BoltConnector.enabled, true );
        builder.setConfig( BoltConnector.encryption_level, OPTIONAL );
        builder.setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) );
        builder.setConfig( GraphDatabaseSettings.tls_key_file, Path.of( NeoInteractionLevel.tempPath( "key", ".key" ) ) );
        builder.setConfig( GraphDatabaseSettings.tls_certificate_file, Path.of( NeoInteractionLevel.tempPath( "cert", ".cert" ) ) );
        builder.setConfig( GraphDatabaseSettings.auth_enabled, true );

        config.forEach( ( setting, valueStr ) ->
        {
            var settingObj = (SettingImpl<Object>) setting;
            builder.setConfig( settingObj, settingObj.parse( valueStr ) );
        } );

        managementService = builder.build();
        db = (GraphDatabaseFacade) managementService.database( DEFAULT_DATABASE_NAME );
        systemDB = (GraphDatabaseFacade) managementService.database( SYSTEM_DATABASE_NAME );
        authManager = db.getDependencyResolver().resolveDependency( CommercialAuthManager.class );
        connectorRegister = db.getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
    }

    @Override
    public EnterpriseUserManager getLocalUserManager() throws Exception
    {
        if ( authManager instanceof CommercialAuthAndUserManager )
        {
            return ((CommercialAuthAndUserManager) authManager).getUserManager();
        }
        throw new Exception( "The configuration used does not have a user manager" );
    }

    @Override
    public GraphDatabaseFacade getLocalGraph()
    {
        return db;
    }

    @Override
    public GraphDatabaseFacade getSystemGraph()
    {
        return systemDB;
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
    public InternalTransaction beginLocalTransactionAsUser( CommercialLoginContext loginContext, KernelTransaction.Type txType )
    {
        return db.beginTransaction( txType, loginContext );
    }

    @Override
    public String executeQuery( CommercialLoginContext loginContext, String call, Map<String,Object> params,
            Consumer<ResourceIterator<Map<String, Object>>> resultConsumer )
    {
        try ( InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.implicit, loginContext ) )
        {
            Map<String,Object> p = (params == null) ? Collections.emptyMap() : params;
            resultConsumer.accept( db.execute( call, p ) );
            tx.success();
            return "";
        }
        catch ( Exception e )
        {
            return e.getMessage();
        }
    }

    @Override
    public CommercialLoginContext login( String username, String password ) throws Exception
    {
        return authManager.login( authToken( username, password ) );
    }

    @Override
    public void logout( CommercialLoginContext loginContext )
    {
        loginContext.subject().logout();
    }

    @Override
    public void updateAuthToken( CommercialLoginContext subject, String username, String password )
    {
    }

    @Override
    public String nameOf( CommercialLoginContext loginContext )
    {
        return loginContext.subject().username();
    }

    @Override
    public void tearDown()
    {
        managementService.shutdown();
    }

    @Override
    public void assertAuthenticated( CommercialLoginContext loginContext )
    {
        assertThat( loginContext.subject().getAuthenticationResult(), equalTo( AuthenticationResult.SUCCESS ) );
    }

    @Override
    public void assertPasswordChangeRequired( CommercialLoginContext loginContext )
    {
        assertThat( loginContext.subject().getAuthenticationResult(), equalTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) );
    }

    @Override
    public void assertInitFailed( CommercialLoginContext loginContext )
    {
        assertThat( loginContext.subject().getAuthenticationResult(), equalTo( AuthenticationResult.FAILURE ) );
    }

    @Override
    public void assertSessionKilled( CommercialLoginContext loginContext )
    {
        // There is no session that could have been killed
    }

    @Override
    public String getConnectionProtocol()
    {
        return "embedded";
    }

    @Override
    public HostnamePort lookupConnector( String connectorKey )
    {
        return connectorRegister.getLocalAddress( connectorKey );
    }
}
