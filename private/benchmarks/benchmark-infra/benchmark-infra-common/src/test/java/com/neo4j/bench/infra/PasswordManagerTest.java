/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.common.results.ErrorReportingPolicy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PasswordManagerTest
{

    @Test
    public void selectCredentialsFromCommandLine()
    {
        // given
        String username = "username1";
        URI uri = URI.create( "http://host1" );
        String password = "password";

        InfraParams infraParams = new InfraParams( new AWSCredentials( "", "", "" ),
                                                   username,
                                                   "secretName",
                                                   password,
                                                   uri,
                                                   URI.create( "http://localhost" ),
                                                   ErrorReportingPolicy.FAIL,
                                                   Workspace.create( Paths.get( "." ) ).build() );
        // when
        ResultStoreCredentials resultStoreCredentials = PasswordManager.getResultStoreCredentials( infraParams );
        // then
        assertEquals( "username1", resultStoreCredentials.username() );
        assertEquals( URI.create( "http://host1" ), resultStoreCredentials.uri() );
        assertEquals( "password", resultStoreCredentials.password() );
    }

    @Test
    public void selectCredentialsFromAWSSecretManager()
    {
        // given
        String username = "username1";
        URI uri = URI.create( "http://host1" );
        String password = null;

        ResultStoreCredentials expectedResultStoreCredentials = new ResultStoreCredentials( "username2",
                                                                                            "password2",
                                                                                            URI.create( "http://host2" ) );

        PasswordManager passwordManager = mock( PasswordManager.class );
        when( passwordManager.getCredentials( "secretName" ) ).thenReturn( expectedResultStoreCredentials );

        InfraParams infraParams = new InfraParams( new AWSCredentials( "", "", "" ),
                                                   username,
                                                   "secretName",
                                                   password,
                                                   uri,
                                                   URI.create( "http://localhost" ),
                                                   ErrorReportingPolicy.FAIL,
                                                   Workspace.create( Paths.get( "." ) ).build() );
        // when
        ResultStoreCredentials resultStoreCredentials = PasswordManager.getResultStoreCredentials( infraParams, passwordManager );
        // then
        assertEquals( "username2", resultStoreCredentials.username() );
        assertEquals( URI.create( "http://host2" ), resultStoreCredentials.uri() );
        assertEquals( "password2", resultStoreCredentials.password() );
    }

    @Test
    public void selectCredentialsFromAWSSecretManagerWithMissingURI()
    {
        // given
        String username = "username1";
        URI uri = URI.create( "http://host1" );

        ResultStoreCredentials expectedResultStoreCredentials = new ResultStoreCredentials( "username2",
                                                                                            "password2",
                                                                                            null );

        PasswordManager passwordManager = mock( PasswordManager.class );
        when( passwordManager.getCredentials( "secretName" ) ).thenReturn( expectedResultStoreCredentials );

        InfraParams infraParams = new InfraParams( new AWSCredentials( "", "", "" ),
                                                   username,
                                                   "secretName",
                                                   null,
                                                   uri,
                                                   URI.create( "http://localhost" ),
                                                   ErrorReportingPolicy.FAIL,
                                                   Workspace.create( Paths.get( "." ) ).build() );
        // when
        ResultStoreCredentials resultStoreCredentials = PasswordManager.getResultStoreCredentials( infraParams, passwordManager );
        // then
        assertEquals( "username1", resultStoreCredentials.username() );
        assertEquals( URI.create( "http://host1" ), resultStoreCredentials.uri() );
        assertEquals( "password2", resultStoreCredentials.password() );
    }

    @Test
    public void failOnMissingCredentials()
    {
        // given
        String username = "username1";
        URI uri = URI.create( "http://host1" );

        ResultStoreCredentials expectedResultStoreCredentials = new ResultStoreCredentials( "username2",
                                                                                            null,
                                                                                            null );

        PasswordManager passwordManager = mock( PasswordManager.class );
        when( passwordManager.getCredentials( "secretName" ) ).thenReturn( expectedResultStoreCredentials );

        InfraParams infraParams = new InfraParams( new AWSCredentials( "", "", "" ),
                                                   username,
                                                   "secretName",
                                                   null,
                                                   uri,
                                                   URI.create( "http://localhost" ),
                                                   ErrorReportingPolicy.FAIL,
                                                   Workspace.create( Paths.get( "." ) ).build() );
        // when
        assertThrows( IllegalArgumentException.class,
                      () -> PasswordManager.getResultStoreCredentials( infraParams, passwordManager ),
                      "" );
    }
}
