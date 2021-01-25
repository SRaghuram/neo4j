/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.server.enterprise.helpers.EnterpriseWebContainerBuilder;
import com.neo4j.server.security.enterprise.auth.NeoInteractionLevel;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.clientconnection.HttpConnectionInfo;
import org.neo4j.server.helpers.CommunityWebContainerBuilder;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.security.CommunityWebContainerTestBase;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.Response;

import static io.netty.channel.local.LocalAddress.ANY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.withBasicAuth;

abstract class AbstractRESTInteraction extends CommunityWebContainerTestBase implements NeoInteractionLevel<RESTSubject>
{
    private final EnterpriseAuthManager authManager;

    abstract String commitPath( String database );

    abstract void consume( Consumer<ResourceIterator<Map<String,Object>>> resultConsumer, JsonNode data );

    abstract HTTP.RawPayload constructQuery( String query );

    protected abstract Response authenticate( String principalCredentials );

    AbstractRESTInteraction( Map<Setting<?>,String> config, Path dataDir ) throws IOException
    {
        Map<String,String> stringMap = new HashMap<>( config.size() );
        config.forEach( ( setting, s ) -> stringMap.put( setting.name(), s ) );

        CommunityWebContainerBuilder builder = EnterpriseWebContainerBuilder.serverOnRandomPorts();
        builder = builder
                .usingDataDir( dataDir.toAbsolutePath().toString() )
                .withProperty( BoltConnector.enabled.name(), TRUE )
                .withProperty( BoltConnector.encryption_level.name(), DISABLED.name() )
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( true ) );

        for ( Map.Entry<String,String> entry : stringMap.entrySet() )
        {
            builder = builder.withProperty( entry.getKey(), entry.getValue() );
        }
        this.testWebContainer = builder.build();
        GraphDatabaseFacade systemDatabase = getSystemGraph();
        DependencyResolver dependencyResolver = systemDatabase.getDependencyResolver();
        authManager = dependencyResolver.resolveDependency( EnterpriseAuthManager.class );
    }

    @Override
    public GraphDatabaseFacade getLocalGraph()
    {
        return testWebContainer.getDefaultDatabase();
    }

    @Override
    public GraphDatabaseFacade getSystemGraph()
    {
        return (GraphDatabaseFacade) this.testWebContainer.getDatabaseManagementService().database( SYSTEM_DATABASE_NAME );
    }

    @Override
    public void shutdown()
    {
        testWebContainer.shutdown();
    }

    @Override
    public FileSystemAbstraction fileSystem()
    {
        return new DefaultFileSystemAbstraction();
    }

    @Override
    public InternalTransaction beginLocalTransactionAsUser( RESTSubject subject, KernelTransaction.Type txType )
            throws Throwable
    {
        LoginContext loginContext = authManager.login( newBasicAuthToken( subject.username, subject.password ) );
        HttpConnectionInfo clientInfo = new HttpConnectionInfo( "testConnection", "http", ANY, ANY, "db/rest" );
        return getLocalGraph().beginTransaction( txType, loginContext, clientInfo );
    }

    @Override
    public InternalTransaction beginLocalTransactionAsUser( RESTSubject subject, KernelTransaction.Type txType, String database )
            throws Throwable
    {
        LoginContext loginContext = authManager.login( newBasicAuthToken( subject.username, subject.password ) );
        HttpConnectionInfo clientInfo = new HttpConnectionInfo( "testConnection", "http", ANY, ANY, "db/rest" );
        DatabaseManagementService managementService = testWebContainer.getDatabaseManagementService();
        var db = (GraphDatabaseFacade) managementService.database( database );
        return db.beginTransaction( txType, loginContext, clientInfo );
    }

    @Override
    public String executeQuery( RESTSubject subject, String database, String call, Map<String,Object> params,
            Consumer<ResourceIterator<Map<String,Object>>> resultConsumer )
    {
        HTTP.RawPayload payload = constructQuery( call );
        Response response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, subject.principalCredentials )
                .POST( commitURL( database ), payload );

        try
        {
            String error = parseErrorMessage( response );
            if ( !error.isEmpty() )
            {
                return error;
            }
            consume( resultConsumer, JsonHelper.jsonNode( response.rawContent() ) );
        }
        catch ( JsonParseException e )
        {
            fail( "Unexpected error parsing Json!" );
        }

        return "";
    }

    @Override
    public RESTSubject login( String username, String password )
    {
        String principalCredentials = HTTP.basicAuthHeader( username, password );
        return new RESTSubject( username, password, principalCredentials );
    }

    @Override
    public void updateAuthToken( RESTSubject subject, String username, String password )
    {
        subject.principalCredentials = HTTP.basicAuthHeader( username, password );
    }

    @Override
    public String nameOf( RESTSubject subject )
    {
        return subject.username;
    }

    @Override
    public void tearDown()
    {
        if ( testWebContainer != null )
        {
            testWebContainer.shutdown();
        }
    }

    @Override
    public void assertAuthenticated( RESTSubject subject )
    {
        Response authenticate = authenticate( subject.principalCredentials );
        assertThat( authenticate.status() ).as( authenticate.rawContent() ).isEqualTo( 200 );
    }

    @Override
    public void assertPasswordChangeRequired( RESTSubject subject )
    {
        // Should be ok to authenticate from REST
        Response authResponse = authenticate( subject.principalCredentials );
        assertThat( authResponse.status() ).isEqualTo( 200 );

        // Should be blocked on data access by the server
        Response dataResponse =
                withBasicAuth( subject.username, subject.password ).POST( testWebContainer.getBaseUri().resolve( txCommitURL() ).toString(),
                        quotedJson( "{'statements':[{'statement':'MATCH (n) RETURN n'}]}" ) );

        try
        {
            assertPermissionErrorAtDataAccess( dataResponse );
        }
        catch ( JsonParseException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void assertUnauthenticated( RESTSubject subject )
    {
        assertThat( authenticate( subject.principalCredentials ).status() ).isNotEqualTo( 200 );
    }

    @Override
    public String getConnectionProtocol()
    {
        return "http";
    }

    @Override
    public void registerTransactionEventListener( String databaseName, TransactionEventListener<?> listener )
    {
        testWebContainer.getDatabaseManagementService().registerTransactionEventListener( databaseName, listener );
    }

    @Override
    public void unregisterTransactionEventListener( String databaseName, TransactionEventListener<?> listener )
    {
        testWebContainer.getDatabaseManagementService().unregisterTransactionEventListener( databaseName, listener );
    }

    private static String parseErrorMessage( Response response )
    {
        try
        {
            JsonNode data = JsonHelper.jsonNode( response.rawContent() );
            if ( data.has( "errors" ) && data.get( "errors" ).has( 0 ) )
            {
                JsonNode firstError = data.get( "errors" ).get( 0 );
                if ( firstError.has( "message" ) )
                {
                    return firstError.get( "message" ).asText();
                }
            }
        }
        catch ( JsonParseException e )
        {
            fail( "Unexpected error parsing Json!" );
        }
        return "";
    }

    String commitURL( String database )
    {
        return testWebContainer.getBaseUri().resolve( commitPath( database ) ).toString();
    }

    abstract class AbstractRESTResult implements ResourceIterator<Map<String,Object>>
    {
        private final JsonNode data;
        private final JsonNode columns;
        private int index;

        AbstractRESTResult( JsonNode fullResult )
        {
            this.data = fullResult.get( "data" );
            this.columns = fullResult.get( "columns" );
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
            JsonNode row = getRow( data, index++ );
            TreeMap<String,Object> map = new TreeMap<>();
            for ( int i = 0; i < columns.size(); i++ )
            {
                String key = columns.get( i ).asText();
                Object value = getValue( row.get( i ) );
                map.put( key, value );
            }
            return map;
        }

        protected abstract JsonNode getRow( JsonNode data, int i );
    }

    private Object getValue( JsonNode valueNode )
    {
        Object value;

        if ( valueNode instanceof TextNode )
        {
            value = valueNode.asText();
        }
        else if ( valueNode instanceof ObjectNode )
        {
            value = mapValue( valueNode.fieldNames(), valueNode );
        }
        else if ( valueNode instanceof ArrayNode )
        {
            ArrayNode aNode = (ArrayNode) valueNode;
            ArrayList<String> listValue = new ArrayList<>( aNode.size() );
            for ( int j = 0; j < aNode.size(); j++ )
            {
                listValue.add( aNode.get( j ).asText() );
            }
            value = listValue;
        }
        else if ( valueNode instanceof IntNode )
        {
            value = valueNode.asInt();
        }
        else if ( valueNode instanceof LongNode )
        {
            value = valueNode.asLong();
        }
        else if ( valueNode instanceof BooleanNode )
        {
            value = valueNode.asBoolean();
        }
        else if ( valueNode.isNull() )
        {
            return null;
        }
        else
        {
            throw new RuntimeException( String.format(
                "Unhandled REST value type '%s'. Need String (TextNode), List (ArrayNode), Object (ObjectNode), " +
                        "long (LongNode), int (IntNode), or bool (BooleanNode).", valueNode.getClass()
            ) );
        }
        return value;
    }

    private Map<String,Object> mapValue( Iterator<String> columns, JsonNode node )
    {
        TreeMap<String,Object> map = new TreeMap<>();
        while ( columns.hasNext() )
        {
            String key = columns.next();
            Object value = getValue( node.get( key ) );
            map.put( key, value );
        }

        return map;
    }
}
