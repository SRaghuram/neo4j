/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.infra.aws.AWSPasswordManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public interface PasswordManager
{

    Logger LOG = LoggerFactory.getLogger( PasswordManager.class );

    static ResultStoreCredentials getResultStoreCredentials( InfraParams infraParams )
    {
        return getResultStoreCredentials( infraParams, AWSPasswordManager.create( infraParams.awsCredentials().awsRegion() ) );
    }

    static ResultStoreCredentials getResultStoreCredentials( ResultStoreCredentials resultStoreCredentials,
                                                             String resultsStoreSecretName,
                                                             PasswordManager passwordManager )
    {
        return getResultStoreCredentials( resultStoreCredentials.username(),
                                          resultStoreCredentials.password(),
                                          resultStoreCredentials.uri(),
                                          resultsStoreSecretName,
                                          passwordManager );
    }

    static ResultStoreCredentials getResultStoreCredentials( InfraParams infraParams, PasswordManager passwordManager )
    {
        return getResultStoreCredentials( infraParams.resultsStoreUsername(),
                                          infraParams.resultsStorePassword(),
                                          infraParams.resultsStoreUri(),
                                          infraParams.resultsStorePasswordSecretName(),
                                          passwordManager );
    }

    static ResultStoreCredentials getResultStoreCredentials( String resultsStoreUsername,
                                                             String resultsStorePassword,
                                                             URI resultsStoreUri,
                                                             String resultsStorePasswordSecretName,
                                                             PasswordManager passwordManager )
    {
        if ( hasAllCredentials( resultsStoreUsername,
                                resultsStorePassword,
                                resultsStoreUri ) )
        {
            LOG.info( "using command line result store credentials" );
            return new ResultStoreCredentials( resultsStoreUsername, resultsStorePassword, resultsStoreUri );
        }

        if ( isNotEmpty( resultsStorePasswordSecretName ) )
        {

            ResultStoreCredentials resultStoreCredentials = passwordManager.getCredentials( resultsStorePasswordSecretName );
            // if result store credentials are all set in AWS SecretsManager use this one,
            // otherwise fallback to URI and username from command line args or fail if not set
            if ( hasAllCredentials( resultStoreCredentials.username(),
                                    resultStoreCredentials.password(),
                                    resultStoreCredentials.uri() ) )
            {
                LOG.info( "using SecretsManager result store credentials" );
                return resultStoreCredentials;
            }
            else if ( hasAllCredentials( resultsStoreUsername,
                                         resultStoreCredentials.password(),
                                         resultsStoreUri ) )
            {
                LOG.info( "using result store password from SecretsManager" );
                return new ResultStoreCredentials( resultsStoreUsername,
                                                   resultStoreCredentials.password(),
                                                   resultsStoreUri );
            }
            else
            {
                throw new IllegalArgumentException( "missing result store credentials" );
            }
        }
        else
        {
            throw new IllegalArgumentException(
                    "invalid result store credentials, either provider username, password and uri or AWS secret name" );
        }
    }

    static Boolean hasAllCredentials( String username, String password, URI uri )
    {
        return isNotEmpty( username ) &&
               isNotEmpty( password ) &&
               uri != null;
    }

    /**
     * It fetches secret value from secret manager.
     *
     * @param secretName
     * @return
     */
    String getSecret( String secretName );

    /**
     * It fetches result store credentials from secret manager
     *
     * @param secretName
     * @return
     */
    ResultStoreCredentials getCredentials( String secretName );
}
