/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.fabric.auth.Credentials;
import com.neo4j.fabric.auth.FabricAuthManagerWrapper;
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.availability.UnavailableException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.SUCCESS;

class CredentialsTest
{

    private final BoltGraphDatabaseManagementServiceSPI databaseManagementService = mock( BoltGraphDatabaseManagementServiceSPI.class );
    private final BoltGraphDatabaseServiceSPI boltDatabaseService = mock( BoltGraphDatabaseServiceSPI.class );
    private final EnterpriseAuthManager commercialAuthManager = mock( EnterpriseAuthManager.class );
    private final EnterpriseLoginContext commercialLoginContext = mock( EnterpriseLoginContext.class );
    private final AuthSubject authSubject = mock( AuthSubject.class );
    private Driver driver;
    private TestServer testServer;

    @BeforeEach
    void setUp() throws InvalidAuthTokenException, UnavailableException
    {
        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "dbms.connector.bolt.listen_address", "0.0.0.0:0",
                "dbms.connector.bolt.enabled", "true"
        );

        var config = org.neo4j.configuration.Config.newBuilder()
                .setRaw( configProperties )
                .build();

        testServer = new TestServer( config );

        testServer.addMocks( databaseManagementService, commercialAuthManager );

        when( commercialAuthManager.login( any() ) ).thenReturn( commercialLoginContext );
        when( commercialLoginContext.subject() ).thenReturn( authSubject );
        when( authSubject.getAuthenticationResult() ).thenReturn( SUCCESS );

        testServer.start();
        when( databaseManagementService.database( any() ) ).thenReturn( boltDatabaseService );
    }

    @AfterEach
    void tearDown()
    {
        testServer.stop();
        if ( driver != null )
        {
            driver.close();
        }
    }

    @Test
    void testNoAuth()
    {
        createDriver( AuthTokens.none() );

        try ( Session session = driver.session() )
        {
            try ( Transaction transaction = session.beginTransaction() )
            {

            }
        }

        ArgumentCaptor<LoginContext> loginContextArgumentCaptor = ArgumentCaptor.forClass( LoginContext.class );
        verify( boltDatabaseService ).beginTransaction( any(), loginContextArgumentCaptor.capture(), any(), any(), any(), any(), any(), any() );

        LoginContext loginContext = loginContextArgumentCaptor.getValue();
        Credentials credentials = FabricAuthManagerWrapper.getCredentials( loginContext.subject() );
        assertFalse( credentials.getProvided() );
        assertNull( credentials.getUsername() );
        assertNull( credentials.getPassword() );
    }

    @Test
    void testProvidedAuth()
    {
        createDriver( AuthTokens.basic( "secret user", "even more secret password" ) );

        try ( Session session = driver.session() )
        {
            try ( Transaction transaction = session.beginTransaction() )
            {

            }
        }

        ArgumentCaptor<LoginContext> loginContextArgumentCaptor = ArgumentCaptor.forClass( LoginContext.class );
        verify( boltDatabaseService ).beginTransaction( any(), loginContextArgumentCaptor.capture(), any(), any(), any(), any(), any(), any() );

        LoginContext loginContext = loginContextArgumentCaptor.getValue();
        Credentials credentials = FabricAuthManagerWrapper.getCredentials( loginContext.subject() );
        assertTrue( credentials.getProvided() );
        assertEquals( "secret user", credentials.getUsername() );
        assertArrayEquals( "even more secret password".getBytes(), credentials.getPassword() );
    }

    private void createDriver( AuthToken authToken )
    {
        driver = GraphDatabase.driver( testServer.getBoltDirectUri(), authToken, Config.builder()
                .withMaxConnectionPoolSize( 1 )
                .withoutEncryption()
                .build() );
    }
}
