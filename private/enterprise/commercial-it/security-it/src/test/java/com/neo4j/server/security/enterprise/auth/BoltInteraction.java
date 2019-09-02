/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.CommercialAuthManager;
import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.bolt.messaging.ResponseMessage;
import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.messaging.response.FailureMessage;
import org.neo4j.bolt.v3.messaging.response.RecordMessage;
import org.neo4j.bolt.v3.messaging.response.SuccessMessage;
import org.neo4j.bolt.v4.messaging.BoltV4Messages;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.clientconnection.BoltConnectionInfo;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

import static io.netty.channel.local.LocalAddress.ANY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.security.AuthToken.BASIC_SCHEME;
import static org.neo4j.kernel.api.security.AuthToken.CREDENTIALS;
import static org.neo4j.kernel.api.security.AuthToken.NATIVE_REALM;
import static org.neo4j.kernel.api.security.AuthToken.PRINCIPAL;
import static org.neo4j.kernel.api.security.AuthToken.REALM_KEY;
import static org.neo4j.kernel.api.security.AuthToken.SCHEME_KEY;
import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;

class BoltInteraction implements NeoInteractionLevel<BoltInteraction.BoltSubject>
{
    private final TransportTestUtil util = new TransportTestUtil();
    private final Factory<TransportConnection> connectionFactory = SocketConnection::new;
    private final Neo4jWithSocket server;
    private Map<String,BoltSubject> subjects = new HashMap<>();
    private FileSystemAbstraction fileSystem;
    private CommercialAuthManager authManager;

    BoltInteraction( Map<Setting<?>,String> config )
    {
        this( config, EphemeralFileSystemAbstraction::new );
    }

    BoltInteraction( Map<Setting<?>,String> config, Supplier<FileSystemAbstraction> fileSystemSupplier )
    {
        TestCommercialDatabaseManagementServiceBuilder factory = new TestCommercialDatabaseManagementServiceBuilder();
        fileSystem = fileSystemSupplier.get();
        server = new Neo4jWithSocket( getClass(),
                factory,
                () -> fileSystem,
                settings ->
                {
                    settings.put( GraphDatabaseSettings.auth_enabled, true );
                    config.forEach( ( setting, value ) -> settings.put( setting, ((SettingImpl<Object>) setting ).parse( value ) )  );
                } );
        server.ensureDatabase( r ->
        {
        } );
        GraphDatabaseFacade db = (GraphDatabaseFacade) server.graphDatabaseService();
        authManager = db.getDependencyResolver().resolveDependency( CommercialAuthManager.class );
    }

    @Override
    public EnterpriseUserManager getLocalUserManager() throws Exception
    {
        if ( authManager instanceof CommercialAuthAndUserManager )
        {
            return ((CommercialAuthAndUserManager) authManager).getUserManager();
        }
        throw new Exception( "The used configuration does not have a user manager" );
    }

    @Override
    public GraphDatabaseFacade getLocalGraph()
    {
        return (GraphDatabaseFacade) server.graphDatabaseService();
    }

    @Override
    public GraphDatabaseFacade getSystemGraph()
    {
        return (GraphDatabaseFacade) server.getManagementService().database( SYSTEM_DATABASE_NAME );
    }

    @Override
    public void shutdown()
    {
        server.shutdownDatabase();
    }

    @Override
    public FileSystemAbstraction fileSystem()
    {
        return fileSystem;
    }

    @Override
    public InternalTransaction beginLocalTransactionAsUser( BoltSubject subject, KernelTransaction.Type txType )
            throws Throwable
    {
        LoginContext loginContext = authManager.login( newBasicAuthToken( subject.username, subject.password ) );
        return getLocalGraph().beginTransaction( txType, loginContext, new BoltConnectionInfo( "testSConnection", "test", ANY, ANY ) );
    }

    @Override
    public String executeQuery( BoltSubject subject, String call, Map<String,Object> params,
            Consumer<ResourceIterator<Map<String,Object>>> resultConsumer )
    {
        if ( params == null )
        {
            params = Collections.emptyMap();
        }
        try
        {
            subject.client.send( util.chunk( BoltV4Messages.run( call, ValueUtils.asMapValue( params ) ), BoltV4Messages.pullAll() ) );
            resultConsumer.accept( collectResults( subject.client ) );
            return "";
        }
        catch ( Exception e )
        {
            return e.getMessage();
        }
    }

    @Override
    public BoltSubject login( String username, String password ) throws Exception
    {
        BoltSubject subject = subjects.get( username );
        if ( subject == null )
        {
            subject = new BoltSubject( connectionFactory.newInstance(), username, password );
            subjects.put( username, subject );
        }
        else
        {
            subject.client.disconnect();
            subject.client = connectionFactory.newInstance();
        }
        subject.client.connect( server.lookupDefaultConnector() )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk( BoltV4Messages.hello(
                        map( REALM_KEY, NATIVE_REALM, PRINCIPAL, username, CREDENTIALS, password,
                                SCHEME_KEY, BASIC_SCHEME ) ) ) );
        assertThat( subject.client, TransportTestUtil.eventuallyReceives( new byte[]{0, 0, 0, 4} ) );
        subject.setLoginResult( util.receiveOneResponseMessage( subject.client ) );
        return subject;
    }

    @Override
    public void logout( BoltSubject subject ) throws Exception
    {
        subject.client.disconnect();
        subject.client = connectionFactory.newInstance();
    }

    @Override
    public void updateAuthToken( BoltSubject subject, String username, String password )
    {

    }

    @Override
    public String nameOf( BoltSubject subject )
    {
        return subject.username;
    }

    @Override
    public void tearDown() throws Throwable
    {
        for ( BoltSubject subject : subjects.values() )
        {
            subject.client.disconnect();
        }
        subjects.clear();
        shutdown();
        fileSystem.close();
    }

    @Override
    public void assertAuthenticated( BoltSubject subject )
    {
        assertTrue( "Should be authenticated", subject.isAuthenticated() );
    }

    @Override
    public void assertPasswordChangeRequired( BoltSubject subject )
    {
        assertTrue( "Should need to change password", subject.passwordChangeRequired() );
    }

    @Override
    public void assertInitFailed( BoltSubject subject )
    {
        assertFalse( "Should not be authenticated", subject.isAuthenticated() );
    }

    @Override
    public void assertSessionKilled( BoltSubject subject )
    {
        assertThat( subject.client, TransportTestUtil.eventuallyDisconnects() );
    }

    @Override
    public String getConnectionProtocol()
    {
        return "bolt";
    }

    @Override
    public HostnamePort lookupConnector( String connectorKey )
    {
        return server.lookupConnector( connectorKey );
    }

    private BoltResult collectResults( TransportConnection client ) throws Exception
    {
        ResponseMessage message = util.receiveOneResponseMessage( client );
        List<String> fieldNames = new ArrayList<>();
        List<Map<String,Object>> result = new ArrayList<>();

        if ( message instanceof SuccessMessage )
        {
            MapValue metadata = ((SuccessMessage) message).meta();
            ListValue fieldNameValues = (ListValue) metadata.get( "fields" );
            for ( AnyValue value : fieldNameValues )
            {
                fieldNames.add( ((TextValue) value).stringValue() );
            }
        }
        else if ( message instanceof FailureMessage )
        {
            FailureMessage failMessage = (FailureMessage) message;
            // drain ignoredMessage, ack failure, get successMessage
            util.receiveOneResponseMessage( client );
            client.send( util.chunk( ResetMessage.INSTANCE ) );
            util.receiveOneResponseMessage( client );
            throw new AuthenticationException( failMessage.status(), failMessage.message() );
        }

        do
        {
            message = util.receiveOneResponseMessage( client );
            if ( message instanceof RecordMessage )
            {
                Object[] row = ((RecordMessage) message).fields();
                Map<String,Object> rowMap = new HashMap<>();
                for ( int i = 0; i < row.length; i++ )
                {
                    rowMap.put( fieldNames.get( i ), row[i] );
                }
                result.add( rowMap );
            }
        }
        while ( !(message instanceof SuccessMessage) && !(message instanceof FailureMessage) );

        if ( message instanceof FailureMessage )
        {
            FailureMessage failMessage = (FailureMessage) message;
            // ack failure, get successMessage
            client.send( util.chunk( ResetMessage.INSTANCE ) );
            util.receiveOneResponseMessage( client );
            throw new AuthenticationException( failMessage.status(), failMessage.message() );
        }

        return new BoltResult( result );
    }

    static class BoltSubject
    {
        TransportConnection client;
        String username;
        String password;
        AuthenticationResult loginResult = AuthenticationResult.FAILURE;

        BoltSubject( TransportConnection client, String username, String password )
        {
            this.client = client;
            this.username = username;
            this.password = password;
        }

        void setLoginResult( ResponseMessage result )
        {
            if ( result instanceof SuccessMessage )
            {
                MapValue meta = ((SuccessMessage) result).meta();
                if ( meta.containsKey( "credentials_expired" ) &&
                     meta.get( "credentials_expired" ).equals( Values.TRUE ) )
                {
                    loginResult = AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
                }
                else
                {
                    loginResult = AuthenticationResult.SUCCESS;
                }
            }
            else if ( result instanceof FailureMessage )
            {
                loginResult = AuthenticationResult.FAILURE;
                Status status = ((FailureMessage) result).status();
                if ( status.equals( Status.Security.AuthenticationRateLimit ) )
                {
                    loginResult = AuthenticationResult.TOO_MANY_ATTEMPTS;
                }
            }
        }

        boolean isAuthenticated()
        {
            return loginResult.equals( AuthenticationResult.SUCCESS );
        }

        boolean passwordChangeRequired()
        {
            return loginResult.equals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
        }
    }

    static class BoltResult implements ResourceIterator<Map<String,Object>>
    {
        private int index;
        private List<Map<String,Object>> data;

        BoltResult( List<Map<String,Object>> data )
        {
            this.data = data;
        }

        @Override
        public void close()
        {
            index = data.size();
        }

        @Override
        public boolean hasNext()
        {
            return index < data.size();
        }

        @Override
        public Map<String,Object> next()
        {
            Map<String,Object> row = data.get( index );
            index++;
            return row;
        }
    }
}
