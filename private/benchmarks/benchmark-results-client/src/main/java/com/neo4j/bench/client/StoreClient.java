/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.client.queries.schema.SetStoreVersion;
import com.neo4j.bench.client.queries.schema.VerifyStoreSchema;

import java.net.URI;
import java.util.function.Supplier;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import static java.lang.String.format;
import static org.neo4j.driver.AccessMode.READ;

public class StoreClient implements AutoCloseable
{
    public static final long VERSION = 14;
    private static final QueryRetrier QUERY_RETRIER = new QueryRetrier( false, QueryRetrier.DEFAULT_TIMEOUT );
    private final Supplier<Driver> driverSupplier;
    private final int retries;
    private Driver driver;

    public static StoreClient connect( URI boltUri, String username, String password )
    {
        return connect( boltUri, username, password, QueryRetrier.DEFAULT_RETRY_COUNT );
    }

    public static StoreClient connect( URI boltUri, String username, String password, int retries )
    {
        AuthToken basicAuth = AuthTokens.basic( username, password );
        Config config = Config.builder()
                              // the following line is required for running tests against Neo4j Server (via Neo4jRule)
                              .withTrustStrategy( Config.TrustStrategy.trustAllCertificates().withoutHostnameVerification() )
                              .withEncryption()
                              .build();
        Supplier<Driver> driverSupplier = () -> GraphDatabase.driver( boltUri, basicAuth, config );
        return QUERY_RETRIER.retry( () ->
        {
            StoreClient storeClient = new StoreClient( driverSupplier, retries );
            try
            {
                storeClient.connect();
                return storeClient;
            }
            catch ( Throwable e )
            {
                // connecting may fail after creating driver, e.g., when setting store version or verifying schema, so driver must be closed
                storeClient.close();
                throw e;
            }
        }, retries );
    }

    private StoreClient( Supplier<Driver> driverSupplier, int retries )
    {
        this.driverSupplier = driverSupplier;
        this.retries = retries;
    }

    @Override
    public void close()
    {
        closeConnection();
    }

    public Session session()
    {
        return driver.session();
    }

    public <RESULT> RESULT execute( Query<RESULT> query )
    {
        return query.execute( driver );
    }

    void reconnect()
    {
        if ( connectionIsLost() )
        {
            closeConnection();
            connect();
        }
    }

    private void connect()
    {
        this.driver = driverSupplier.get();
        setStoreVersionIfAbsent();
        // run in retry loop because verification is racy
        QUERY_RETRIER.execute( this, new VerifyStoreSchema(), retries );
    }

    private boolean connectionIsLost()
    {
        if ( driver != null )
        {
            try ( Session session = driver.session( SessionConfig.builder().withDefaultAccessMode( READ ).build() ) )
            {
                return !session.run( "RETURN 1" ).hasNext();
            }
            catch ( Throwable e )
            {
                return true;
            }
        }
        return true;
    }

    private void closeConnection()
    {
        if ( driver != null )
        {
            try
            {
                driver.close();
            }
            catch ( Throwable e )
            {
                throw new RuntimeException( "Error while closing driver", e );
            }
        }
    }

    private void setStoreVersionIfAbsent()
    {
        int count = versionNodeCount();
        switch ( count )
        {
        case 0:
            QUERY_RETRIER.execute( this, new SetStoreVersion( VERSION ) );
            break;
        case 1:
            return;
        default:
            throw new RuntimeException( format( "Expected 1 store version node, but found %s", count ) );
        }
    }

    private int versionNodeCount()
    {
        try ( Session session = driver.session( SessionConfig.builder().withDefaultAccessMode( READ ).build() ) )
        {
            return session.run( "MATCH (ss:StoreSchema) RETURN count(ss) AS c" ).single().get( "c" ).asInt();
        }
    }
}
