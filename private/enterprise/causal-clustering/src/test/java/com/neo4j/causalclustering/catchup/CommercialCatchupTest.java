/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.FileChunk;
import com.neo4j.causalclustering.catchup.storecopy.FileHeader;
import com.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponse;
import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import com.neo4j.causalclustering.catchup.tx.TxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.catchup.v1.CatchupProtocolClientInstallerV1;
import com.neo4j.causalclustering.catchup.v1.CatchupProtocolServerInstallerV1;
import com.neo4j.causalclustering.catchup.v1.storecopy.GetStoreIdRequest;
import com.neo4j.causalclustering.catchup.v2.CatchupProtocolClientInstallerV2;
import com.neo4j.causalclustering.catchup.v2.CatchupProtocolServerInstallerV2;
import com.neo4j.causalclustering.catchup.v3.storecopy.CatchupProtocolClientInstallerV3;
import com.neo4j.causalclustering.catchup.v3.storecopy.CatchupProtocolServerInstallerV3;
import com.neo4j.causalclustering.common.StubLocalDatabaseService;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import com.neo4j.causalclustering.identity.StoreId;
import com.neo4j.causalclustering.messaging.DatabaseCatchupRequest;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.Protocol;
import io.netty.channel.embedded.EmbeddedChannel;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith( DefaultFileSystemExtension.class )
abstract class CommercialCatchupTest
{
    private static final String DEFAULT_DB = "db-one";
    private final Protocol.ApplicationProtocols applicationProtocols;
    private DatabaseManager databaseManager;

    CommercialCatchupTest( Protocol.ApplicationProtocols applicationProtocol )
    {
        applicationProtocols = applicationProtocol;
    }

    @Inject
    private DefaultFileSystemAbstraction fsa;
    private final EmbeddedChannel client = new EmbeddedChannel();
    private final EmbeddedChannel server = new EmbeddedChannel();

    private static final LogProvider LOG_PROVIDER = NullLogProvider.getInstance();
    private NettyPipelineBuilderFactory pipelineBuilderFactory;
    private MultiDatabaseCatchupServerHandler serverResponseHandler;

    @BeforeEach
    void setUp()
    {
        pipelineBuilderFactory = new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER );
        databaseManager = TestDatabaseManager.newDbManager( DEFAULT_DB );
        serverResponseHandler = new MultiDatabaseCatchupServerHandler( () -> databaseManager, LOG_PROVIDER, fsa );
    }

    void executeTestScenario( Function<DatabaseManager,RequestResponse> responseFunction ) throws Exception
    {
        RequestResponse requestResponse = responseFunction.apply( databaseManager );

        installChannels( pipelineBuilderFactory, requestResponse.responseHandler, serverResponseHandler );

        client.writeOutbound( requestResponse.request.messageType() );
        client.writeOutbound( requestResponse.request );
        Object bytebuf;
        while ( (bytebuf = client.readOutbound()) != null )
        {
            server.writeInbound( bytebuf );
        }
        while ( (bytebuf = server.readOutbound()) != null )
        {
            client.writeInbound( bytebuf );
        }

        requestResponse.responseHandler.verifyHandled();
    }

    static Function<DatabaseManager,RequestResponse> storeId()
    {
        return new Function<DatabaseManager,RequestResponse>()
        {
            @Override
            public RequestResponse apply( DatabaseManager databaseManager )
            {
                return new RequestResponse( new GetStoreIdRequest( DEFAULT_DB ), new ResponseAdaptor()
                {
                    @Override
                    public void onGetStoreIdResponse( GetStoreIdResponse response )
                    {
                        responseHandled = true;
                        assertEquals( databaseManager
                                .getDatabaseContext( DEFAULT_DB )
                                .map( DatabaseContext::getDatabase )
                                .map( db -> new StoreId( db.getStoreId().getCreationTime(), db.getStoreId().getRandomId(), db.getStoreId().getUpgradeTime(),
                                        db.getStoreId().getUpgradeId() ) )
                                .orElseThrow( IllegalStateException::new ), response.storeId() );
                    }
                } );
            }

            @Override
            public String toString()
            {
                // implemented to give parameterized tests nicer names
                return "storeId";
            }
        };
    }

    static Function<DatabaseManager,RequestResponse> wrongDb()
    {
        return new Function<DatabaseManager,RequestResponse>()
        {
            @Override
            public RequestResponse apply( DatabaseManager databaseManager )
            {
                return new RequestResponse( new GetStoreIdRequest( "WRONG_DB_NAME" ), new ResponseAdaptor()
                {
                    @Override
                    public void onCatchupErrorResponse( CatchupErrorResponse catchupErrorResponse )
                    {
                        responseHandled = true;
                        assertEquals( CatchupResult.E_DATABASE_UNKNOWN, catchupErrorResponse.status() );
                        assertThat( catchupErrorResponse.message(), CoreMatchers.containsString( "database WRONG_DB_NAME does not exist on this machine." ) );
                    }
                } );
            }

            @Override
            public String toString()
            {
                // implemented to give parameterized tests nicer names
                return "wrongDb";
            }
        };
    }

    static class RequestResponse
    {
        private final DatabaseCatchupRequest request;
        final ResponseAdaptor responseHandler;

        RequestResponse( DatabaseCatchupRequest request, ResponseAdaptor responseHandler )
        {
            this.request = request;
            this.responseHandler = responseHandler;
        }
    }

    private void installChannels( NettyPipelineBuilderFactory pipelineBuilderFactory, CatchupResponseHandler catchupResponseHandler,
            MultiDatabaseCatchupServerHandler responseHandler ) throws Exception
    {
        if ( applicationProtocols == Protocol.ApplicationProtocols.CATCHUP_1 )
        {
            new CatchupProtocolClientInstallerV1( pipelineBuilderFactory, Collections.emptyList(), LOG_PROVIDER, catchupResponseHandler ).install( client );
            new CatchupProtocolServerInstallerV1( pipelineBuilderFactory, Collections.emptyList(), LOG_PROVIDER, responseHandler, DEFAULT_DB ).install(
                    server );
        }
        else if ( applicationProtocols == Protocol.ApplicationProtocols.CATCHUP_2 )
        {
            new CatchupProtocolClientInstallerV2( pipelineBuilderFactory, Collections.emptyList(), LOG_PROVIDER, catchupResponseHandler ).install( client );
            new CatchupProtocolServerInstallerV2( pipelineBuilderFactory, Collections.emptyList(), LOG_PROVIDER, responseHandler ).install( server );
        }
        else if ( applicationProtocols == Protocol.ApplicationProtocols.CATCHUP_3 )
        {
            new CatchupProtocolClientInstallerV3( pipelineBuilderFactory, Collections.emptyList(), LOG_PROVIDER, catchupResponseHandler ).install( client );
            new CatchupProtocolServerInstallerV3( pipelineBuilderFactory, Collections.emptyList(), LOG_PROVIDER, responseHandler ).install( server );
        }
        else
        {
            throw new IllegalArgumentException( "Unsupported protocol " + applicationProtocols );
        }
    }

    static class StubDatabaseManager implements DatabaseManager
    {
        private final StubLocalDatabaseService databaseService;

        StubDatabaseManager( StubLocalDatabaseService databaseService )
        {
            this.databaseService = databaseService;
        }

        @Override
        public Optional<DatabaseContext> getDatabaseContext( String name )
        {
            return databaseService.get( name ).map( d -> new DatabaseContext( d.database(), new GraphDatabaseFacade() ) );
        }

        @Override
        public DatabaseContext createDatabase( String databaseName )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropDatabase( String databaseName )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void stopDatabase( String databaseName )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startDatabase( String databaseName )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listDatabases()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void init() throws Throwable
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void start() throws Throwable
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void stop() throws Throwable
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() throws Throwable
        {
            throw new UnsupportedOperationException();
        }
    }

    static class ResponseAdaptor implements CatchupResponseHandler
    {
        boolean responseHandled;

        @Override
        public void onFileHeader( FileHeader fileHeader )
        {
            unexpected();
        }

        @Override
        public boolean onFileContent( FileChunk fileChunk ) throws IOException
        {
            unexpected();
            return false;
        }

        @Override
        public void onFileStreamingComplete( StoreCopyFinishedResponse response )
        {
            unexpected();
        }

        @Override
        public void onTxPullResponse( TxPullResponse tx )
        {
            unexpected();
        }

        @Override
        public void onTxStreamFinishedResponse( TxStreamFinishedResponse response )
        {
            unexpected();
        }

        @Override
        public void onGetStoreIdResponse( GetStoreIdResponse response )
        {
            unexpected();
        }

        @Override
        public void onCoreSnapshot( CoreSnapshot coreSnapshot )
        {
            unexpected();
        }

        @Override
        public void onStoreListingResponse( PrepareStoreCopyResponse storeListingRequest )
        {
            unexpected();
        }

        @Override
        public void onCatchupErrorResponse( CatchupErrorResponse catchupErrorResponse )
        {
            unexpected();
        }

        @Override
        public void onClose()
        {
            unexpected();
        }

        void unexpected()
        {
            fail( "Not an expected responseHandler" );
        }

        void verifyHandled()
        {
            assertTrue( responseHandled );
        }
    }
}
