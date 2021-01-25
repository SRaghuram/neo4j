/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.internal.kernel.api.security.AuthSubject;

public class ExternalCredentialsProvider implements CredentialsProvider
{
    @Override
    public AuthToken credentialsFor( EnterpriseLoginContext loginContext )
    {
        return credentialsFor( loginContext.subject(), Values::value, InternalAuthToken::new );
    }

    // this method is for use in integration tests
    // it represents the functionality with driver types erased
    // since the driver is shaded, its types should not be used outside this module
    public <T, V> T credentialsFor( AuthSubject subject, Function<Object,V> valueMapper, Function<Map<String,V>,T> tokenProducer )
    {
        Map<String,Object>  interceptedAuthToken = FabricAuthManagerWrapper.getInterceptedAuthToken( subject );

        Map<String,V> convertedAuthToken = new HashMap<>();

        interceptedAuthToken.forEach( ( key, value ) ->
        {
            if ( org.neo4j.kernel.api.security.AuthToken.CREDENTIALS.equals( key ) )
            {
                // credentials are passed around in the server as byte[]
                convertedAuthToken.put( key, valueMapper.apply( new String( (byte[]) value ) ) );
            }
            else
            {
                convertedAuthToken.put( key, valueMapper.apply( value ) );
            }
        } );

        return tokenProducer.apply( convertedAuthToken );
    }
}
