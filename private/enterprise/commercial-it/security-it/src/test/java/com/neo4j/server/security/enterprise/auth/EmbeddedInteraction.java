/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.CommercialAuthManager;
import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import com.neo4j.test.TestCommercialGraphDatabaseFactory;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.configuration.BoltConnector;
import org.neo4j.configuration.ConnectorPortRegister;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.LegacySslPolicyConfig;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.configuration.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

public class EmbeddedInteraction implements NeoInteractionLevel<CommercialLoginContext>
{
    protected GraphDatabaseFacade db;
    private CommercialAuthManager authManager;
    private FileSystemAbstraction fileSystem;
    private ConnectorPortRegister connectorRegister;

    EmbeddedInteraction( Map<String, String> config ) throws Throwable
    {
        this( config, EphemeralFileSystemAbstraction::new );
    }

    EmbeddedInteraction( Map<String, String> config, Supplier<FileSystemAbstraction> fileSystemSupplier ) throws Throwable
    {
        TestCommercialGraphDatabaseFactory factory = new TestCommercialGraphDatabaseFactory();
        this.fileSystem = fileSystemSupplier.get();
        factory.setFileSystem( fileSystem );
        GraphDatabaseBuilder builder = factory.newImpermanentDatabaseBuilder();
        init( builder, config );
    }

    public EmbeddedInteraction( GraphDatabaseBuilder builder, Map<String, String> config ) throws Throwable
    {
        init( builder, config );
    }

    protected EmbeddedInteraction()
    {
    }

    protected void init( GraphDatabaseBuilder builder, Map<String, String> config ) throws Throwable
    {
        builder.setConfig( new BoltConnector( "bolt" ).type, "BOLT" );
        builder.setConfig( new BoltConnector( "bolt" ).enabled, "true" );
        builder.setConfig( new BoltConnector( "bolt" ).encryption_level, OPTIONAL.name() );
        builder.setConfig( new BoltConnector( "bolt" ).listen_address, "localhost:0" );
        builder.setConfig( LegacySslPolicyConfig.tls_key_file, NeoInteractionLevel.tempPath( "key", ".key" ) );
        builder.setConfig( LegacySslPolicyConfig.tls_certificate_file,
                NeoInteractionLevel.tempPath( "cert", ".cert" ) );
        builder.setConfig( GraphDatabaseSettings.auth_enabled, "true" );

        builder.setConfig( config );

        db = (GraphDatabaseFacade) builder.newGraphDatabase();
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
    public FileSystemAbstraction fileSystem()
    {
        return fileSystem;
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
        db.shutdown();
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
