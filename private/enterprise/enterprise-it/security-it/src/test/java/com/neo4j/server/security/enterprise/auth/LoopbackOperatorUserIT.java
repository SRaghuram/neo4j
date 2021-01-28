/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.commandline.admin.security.SetOperatorPasswordCommand;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintStream;
import java.net.Inet4Address;
import java.net.SocketException;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.client.UnixDomainSocketConnection;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.admin.security.SetInitialPasswordCommand;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.testing.MessageConditions.msgFailure;
import static org.neo4j.bolt.testing.MessageConditions.msgRecord;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.StreamConditions.eqRecord;
import static org.neo4j.bolt.testing.TransportTestUtil.anyValue;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyReceives;
import static org.neo4j.bolt.testing.TransportTestUtil.stringEquals;
import static org.neo4j.bolt.v4.BoltProtocolV4ComponentFactory.newMessageEncoder;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;

/**
 * Note: This test uses low level bolt messages since the driver does not support Unix Domain Sockets.
 */
@EnabledOnOs( OS.LINUX )
@ExtendWith( EphemeralFileSystemExtension.class )
class LoopbackOperatorUserIT
{
    private static final Path LISTEN_FILE = Path.of( "/tmp/loopy.sock" );
    private static final TransportTestUtil util = new TransportTestUtil( newMessageEncoder() );
    private static final org.neo4j.driver.Config driverConfig = org.neo4j.driver.Config.builder()
                                                                                       .withLogging( Logging.none() )
                                                                                       .withoutEncryption()
                                                                                       .withConnectionTimeout( 10, TimeUnit.SECONDS )
                                                                                       .withMaxConnectionPoolSize( 1 )
                                                                                       .withEventLoopThreads( 1 )
                                                                                       .build();

    @Inject
    private EphemeralFileSystemAbstraction fileSystem;
    private Path confDir;
    private Path homeDir;
    private PrintStream out;
    private PrintStream err;
    private DatabaseManagementService enterpriseDbms;
    private TransportConnection unixSocket;

    @BeforeEach
    void setup()
    {
        Path graphDir = Path.of( DEFAULT_DATABASE_NAME ).resolve( UUID.randomUUID().toString() );
        confDir = graphDir.resolve( "conf" );
        homeDir = graphDir.resolve( "home" );
        out = mock( PrintStream.class );
        err = mock( PrintStream.class );
    }

    @AfterEach
    void tearDown() throws Exception
    {
        if ( enterpriseDbms != null )
        {
            enterpriseDbms.shutdown();
        }
        if ( unixSocket != null )
        {
            unixSocket.disconnect();
        }
        fileSystem.close();
    }

    @Test
    void shouldConnectAsOperatorRestrictionEnabled() throws Exception
    {
        // GIVEN
        setupRestricted();

        establishConnection();
        authenticateAsLoopback();

        unixSocket.send( util.defaultRunAutoCommitTx( "CALL dbms.showCurrentUser() YIELD username, roles",
                                                      ValueUtils.asMapValue( emptyMap() ), DEFAULT_DATABASE_NAME ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgRecord( eqRecord( stringEquals( "" ), anyValue() ) ) ) );

        unixSocket.send( util.defaultRunAutoCommitTx( "CALL dbms.showCurrentUser() YIELD username, roles",
                                                      ValueUtils.asMapValue( emptyMap() ), SYSTEM_DATABASE_NAME ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgRecord( eqRecord( stringEquals( "" ), anyValue() ) ) ) );
    }

    @Test
    void shouldNotAuthenticateOperatorOnNormalPort()
    {
        // GIVEN
        setupRestricted();
        var ip = Inet4Address.getLoopbackAddress().getHostAddress();
        ConnectorPortRegister portRegister =
                ((GraphDatabaseAPI) enterpriseDbms.database( DEFAULT_DATABASE_NAME )).getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        HostnamePort bolt = portRegister.getLocalAddress( "bolt" );

        // WHEN
        String uri = "neo4j://" + ip + ":" + bolt.getPort();
        assertThatThrownBy( () ->
                            {
                                Driver driver = GraphDatabase.driver( uri, AuthTokens.basic( "IGNORED", "bar" ), driverConfig );
                                driver.verifyConnectivity();
                            } ).isInstanceOf( AuthenticationException.class );
    }

    @Test
    void shouldNotConnectAsOperatorRestrictionDisabled()
    {
        // GIVEN
        setupUnrestricted();

        //WHEN
        assertThrows( SocketException.class, this::establishConnection );
    }

    @ParameterizedTest
    @ValueSource( strings = {"dbms.upgrade", "dbms.upgradeStatus", "dbms.upgradeDetails", "dbms.upgradeStatusDetails"} )
    void shouldOnlyAllowOperatorUserToCallUpgradeProcedure( String procedureName ) throws Exception
    {
        String query = String.format( "CALL %s()", procedureName );
        setInitialPassword();
        setupRestricted();

        establishConnection();
        authenticateAsLoopback();

        unixSocket.send( util.defaultRunAutoCommitTx( query,
                                                      ValueUtils.asMapValue( emptyMap() ), SYSTEM_DATABASE_NAME ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceivesSuccessAfterRecords() );

        assertThatThrownBy( () ->
                            {
                                Session session = loginBolt();
                                session.run( query ).consume();
                            } ).hasMessageContaining( "Executing procedure is not allowed for user" );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldAllowOperatorUserToCreateDatabase( boolean blocked ) throws Exception
    {
        // GIVEN
        setupRestricted( blocked );

        // WHEN
        establishConnection();
        authenticateAsLoopback();

        runAndConsume( "CREATE DATABASE operational" );

        // THEN
        assertDatabasesCreated();
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldAllowOperatorUserToCreateDatabaseIfNotExists( boolean blocked ) throws Exception
    {
        // GIVEN
        setupRestricted( blocked );

        // WHEN
        establishConnection();
        authenticateAsLoopback();

        runAndConsume( String.format( "CREATE DATABASE %s IF NOT EXISTS", DEFAULT_DATABASE_NAME ) );
        runAndConsume( "CREATE DATABASE operational IF NOT EXISTS" );

        // THEN
        assertDatabasesCreated();
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldAllowOperatorUserToCreateOrReplaceDatabase( boolean blocked ) throws Exception
    {
        // GIVEN
        setupRestricted( blocked );

        // WHEN
        establishConnection();
        authenticateAsLoopback();

        runAndConsume( String.format( "CREATE OR REPLACE DATABASE %s", DEFAULT_DATABASE_NAME ) );
        runAndConsume( "CREATE OR REPLACE DATABASE operational" );

        // THEN
        assertDatabasesCreated();
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldAllowOperatorUserToDropDatabase( boolean blocked ) throws Exception
    {
        // GIVEN
        setupRestricted( blocked );

        // WHEN
        establishConnection();
        authenticateAsLoopback();

        runAndConsume( String.format( "DROP DATABASE %s", DEFAULT_DATABASE_NAME ) );

        // THEN
        assertOnlySystemDBPresent( unixSocket );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldAllowOperatorUserToDropDatabaseIfExists( boolean blocked ) throws Exception
    {
        // GIVEN
        setupRestricted( blocked );

        // WHEN
        establishConnection();
        authenticateAsLoopback();

        runAndConsume( String.format( "DROP DATABASE %s IF EXISTS", DEFAULT_DATABASE_NAME ) );
        runAndConsume( "DROP DATABASE operational IF EXISTS" );

        // THEN
        assertOnlySystemDBPresent( unixSocket );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldAllowOperatorUserToStartAndStopDatabase( boolean blocked ) throws Exception
    {
        // GIVEN
        setupRestricted( blocked );

        // WHEN
        establishConnection();
        authenticateAsLoopback();

        // Stop online database
        runAndConsume( String.format( "STOP DATABASE %s", DEFAULT_DATABASE_NAME ) );

        // THEN
        assertDefaultDatabaseOffline();

        // WHEN - Stop offline database
        runAndConsume( String.format( "STOP DATABASE %s", DEFAULT_DATABASE_NAME ) );

        // THEN
        assertDefaultDatabaseOffline();

        // WHEN - Start offline database
        runAndConsume( String.format( "START DATABASE %s", DEFAULT_DATABASE_NAME ) );

        // THEN
        assertDefaultDatabaseOnline();

        // WHEN - Start online database
        runAndConsume( String.format( "START DATABASE %s", DEFAULT_DATABASE_NAME ) );

        // THEN
        assertDefaultDatabaseOnline();
    }

    @Test
    void shouldNotListUpgradeUser()
    {
        setInitialPassword();
        setupRestricted();

        try ( Session session = loginBolt() )
        {
            Record user = session.run( "SHOW USERS" ).single();
            assertThat( user.get( "user" ).asString() ).isEqualTo( INITIAL_USER_NAME );
        }
    }

    @Test
    void shouldFailAlterPasswordUpgradeUser() throws Exception
    {
        //GIVEN
        setupRestricted();
        establishConnection();
        authenticateAsLoopback();

        // WHEN
        unixSocket.send( util.defaultRunAutoCommitTx( "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO 'foo'",
                                                      ValueUtils.asMapValue( emptyMap() ), SYSTEM_DATABASE_NAME ) );

        // THEN
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgFailure( Status.Statement.ExecutionFailed,
                                                                                 "User failed to alter their own password: " +
                                                                                 "Command not available with auth disabled." ) ) );
    }

    @Test
    void shouldFailStartupWithNoOperatorPassword()
    {
        assertThatThrownBy( () -> getEnterpriseManagementService( Map.of( BoltConnectorInternalSettings.enable_loopback_auth, true,
                                                                          BoltConnectorInternalSettings.unsupported_loopback_listen_file, LISTEN_FILE  ) )
        )
                .hasRootCauseMessage( "No password has been set for the loopback operator. Run `neo4j-admin set-operator-password <password>`." );
    }

    private Session loginBolt()
    {
        var ip = Inet4Address.getLoopbackAddress().getHostAddress();
        ConnectorPortRegister portRegister =
                ((GraphDatabaseAPI) enterpriseDbms.database( DEFAULT_DATABASE_NAME )).getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        HostnamePort bolt = portRegister.getLocalAddress( "bolt" );
        String uri = "neo4j://" + ip + ":" + bolt.getPort();
        Driver driver = GraphDatabase.driver( uri, AuthTokens.basic( INITIAL_USER_NAME, "foo" ), driverConfig );
        driver.verifyConnectivity();
        return driver.session( SessionConfig.builder().withDatabase( SYSTEM_DATABASE_NAME ).build() );
    }

    private DatabaseManagementService getEnterpriseManagementService( Map<Setting<?>,Object> config )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( homeDir )
                .setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.encryption_level, DISABLED )
                .setConfig( BoltConnector.listen_address, new SocketAddress( "0.0.0.0", 0 ) )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) )
                .setConfig( GraphDatabaseSettings.auth_enabled, true )
                .setConfig( config )
                .build();
    }

    private void setupRestricted()
    {
        setupOperatorUserAndSystemDatabase( true, true );
    }

    private void setupRestricted( boolean blocked )
    {
        setupOperatorUserAndSystemDatabase( true, blocked );
    }

    private void setupUnrestricted()
    {
        setupOperatorUserAndSystemDatabase( false, false );
    }

    private void setupOperatorUserAndSystemDatabase( boolean restrictUpgrade, boolean blockDatabase )
    {
        setOperatorPassword();
        final Map<Setting<?>,Object> config =
                Map.of( BoltConnectorInternalSettings.enable_loopback_auth, restrictUpgrade || blockDatabase,
                        BoltConnectorInternalSettings.unsupported_loopback_listen_file, LISTEN_FILE,
                        GraphDatabaseInternalSettings.block_upgrade_procedures, restrictUpgrade,
                        GraphDatabaseInternalSettings.block_create_drop_database, blockDatabase,
                        GraphDatabaseInternalSettings.block_start_stop_database, blockDatabase );

        enterpriseDbms = getEnterpriseManagementService( config );
        enterpriseDbms.database( SYSTEM_DATABASE_NAME );
    }

    private void setInitialPassword()
    {
        final var ctx = new ExecutionContext( homeDir, confDir, out, err, fileSystem );
        final var command = new SetInitialPasswordCommand( ctx );
        CommandLine.populateCommand( command, "foo" );
        command.execute();
    }

    private void setOperatorPassword()
    {
        final var ctx = new ExecutionContext( homeDir, confDir, out, err, fileSystem );
        final var command = new SetOperatorPasswordCommand( ctx );
        CommandLine.populateCommand( command, "bar" );
        command.execute();
    }

    private void assertDatabasesCreated() throws IOException
    {
        unixSocket.send( util.defaultRunAutoCommitTx( "SHOW DATABASES",
                                                      ValueUtils.asMapValue( emptyMap() ), SYSTEM_DATABASE_NAME ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgRecord( eqRecord( stringEquals( "neo4j" ), anyValue(), anyValue(), anyValue(),
                                                                                          anyValue(), anyValue(), anyValue(), anyValue() ) ) ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgRecord( eqRecord( stringEquals( "operational" ), anyValue(), anyValue(),
                                                                                          anyValue(), anyValue(), anyValue(), anyValue(), anyValue() ) ) ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgRecord( eqRecord( stringEquals( "system" ), anyValue(), anyValue(), anyValue(),
                                                                                          anyValue(), anyValue(), anyValue(), anyValue() ) ) ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    private void authenticateAsLoopback() throws IOException
    {
        unixSocket.send( util.defaultAuth( map( "scheme", "basic", "principal", "IGNORED", "credentials", "bar" ) ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    private void runAndConsume( String cypherQuery ) throws IOException
    {
        unixSocket.send( util.defaultRunAutoCommitTx( cypherQuery,
                                                      ValueUtils.asMapValue( emptyMap() ), SYSTEM_DATABASE_NAME ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    private void assertOnlySystemDBPresent( TransportConnection unixSocket ) throws IOException
    {
        unixSocket.send( util.defaultRunAutoCommitTx( "SHOW DATABASES",
                                                      ValueUtils.asMapValue( emptyMap() ), SYSTEM_DATABASE_NAME ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgRecord( eqRecord( stringEquals( "system" ), anyValue(), anyValue(), anyValue(),
                                                                                          anyValue(), anyValue(), anyValue(), anyValue() ) ) ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    private void assertDefaultDatabaseOffline() throws IOException
    {
        unixSocket.send( util.defaultRunAutoCommitTx( "SHOW DEFAULT DATABASE",
                                                      ValueUtils.asMapValue( emptyMap() ), SYSTEM_DATABASE_NAME ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgRecord( eqRecord( stringEquals( DEFAULT_DATABASE_NAME ), anyValue(), anyValue(),
                                                                                          stringEquals( "offline" ), stringEquals( "offline" ),
                                                                                          anyValue() ) ) ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    private void assertDefaultDatabaseOnline() throws IOException
    {
        unixSocket.send( util.defaultRunAutoCommitTx( "SHOW DEFAULT DATABASE",
                                                      ValueUtils.asMapValue( emptyMap() ), SYSTEM_DATABASE_NAME ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgRecord( eqRecord( stringEquals( DEFAULT_DATABASE_NAME ), anyValue(), anyValue(),
                                                                                          stringEquals( "online" ), stringEquals( "online" ),
                                                                                          anyValue() ) ) ) );
        assertThat( unixSocket ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    private void establishConnection() throws Exception
    {
        unixSocket = new UnixDomainSocketConnection();

        unixSocket.connect( new AFUNIXSocketAddress( LISTEN_FILE.toFile() ) );

        unixSocket.send( util.defaultAcceptedVersions() );
        assertThat( unixSocket ).satisfies( eventuallyReceives( new byte[]{0, 0, 2, 4} ) );
    }
}
