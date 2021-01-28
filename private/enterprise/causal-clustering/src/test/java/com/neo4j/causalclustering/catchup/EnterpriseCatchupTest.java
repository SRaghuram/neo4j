/*
 * Copyright (c) "Neo4j"
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
import com.neo4j.causalclustering.catchup.v3.CatchupProtocolClientInstallerV3;
import com.neo4j.causalclustering.catchup.v3.CatchupProtocolServerInstallerV3;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdResponse;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreIdRequest;
import com.neo4j.causalclustering.catchup.v4.CatchupProtocolClientInstallerV4;
import com.neo4j.causalclustering.catchup.v4.CatchupProtocolServerInstallerV4;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsResponse;
import com.neo4j.causalclustering.catchup.v4.info.InfoResponse;
import com.neo4j.causalclustering.catchup.v4.metadata.GetMetadataResponse;
import com.neo4j.causalclustering.catchup.v5.CatchupProtocolClientInstallerV5;
import com.neo4j.causalclustering.catchup.v5.CatchupProtocolServerInstallerV5;
import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import io.netty.channel.embedded.EmbeddedChannel;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.function.Function;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler.catchupServerHandler;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

@ExtendWith( DefaultFileSystemExtension.class )
abstract class EnterpriseCatchupTest
{
    private static final TestDatabaseIdRepository DATABASE_ID_REPOSITORY = new TestDatabaseIdRepository();
    private static final NamedDatabaseId DEFAULT_NAMED_DB_ID = DATABASE_ID_REPOSITORY.defaultDatabase();
    private final ApplicationProtocols applicationProtocols;
    private StubClusteredDatabaseManager databaseManager;

    EnterpriseCatchupTest( ApplicationProtocols applicationProtocol )
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
        var availabilityGuard = mock( DatabaseAvailabilityGuard.class );
        doReturn( true ).when( availabilityGuard ).isAvailable();

        pipelineBuilderFactory = NettyPipelineBuilderFactory.insecure();
        databaseManager = new StubClusteredDatabaseManager( DATABASE_ID_REPOSITORY );
        databaseManager.givenDatabaseWithConfig()
                .withDatabaseId( DEFAULT_NAMED_DB_ID )
                .withStoreId( StoreId.UNKNOWN )
                .withDatabaseAvailabilityGuard( availabilityGuard )
                .register();
        final var dependencyResolver = mock( DependencyResolver.class );
        serverResponseHandler = catchupServerHandler( databaseManager, mock( DatabaseStateService.class ), fsa, 32768, LOG_PROVIDER, dependencyResolver );
    }

    void executeTestScenario( Function<DatabaseManager<?>,RequestResponse> responseFunction ) throws Exception
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

    static Function<DatabaseManager<?>,RequestResponse> storeId()
    {
        return new Function<>()
        {
            @Override
            public RequestResponse apply( DatabaseManager<?> databaseManager )
            {
                return new RequestResponse( new GetStoreIdRequest( DEFAULT_NAMED_DB_ID.databaseId() ), new ResponseAdaptor()
                {
                    @Override
                    public void onGetStoreIdResponse( GetStoreIdResponse response )
                    {
                        responseHandled = true;
                        assertEquals( databaseManager.getDatabaseContext( DEFAULT_NAMED_DB_ID )
                                .map( DatabaseContext::database )
                                .map( Database::getStoreId )
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

    static Function<DatabaseManager<?>,RequestResponse> wrongDb()
    {
        return new Function<>()
        {
            @Override
            public RequestResponse apply( DatabaseManager<?> databaseManager )
            {
                return new RequestResponse( new GetStoreIdRequest( randomNamedDatabaseId().databaseId() ), new ResponseAdaptor()
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
        private final CatchupProtocolMessage.WithDatabaseId request;
        final ResponseAdaptor responseHandler;

        RequestResponse( CatchupProtocolMessage.WithDatabaseId request, ResponseAdaptor responseHandler )
        {
            this.request = request;
            this.responseHandler = responseHandler;
        }
    }

    private void installChannels( NettyPipelineBuilderFactory pipelineBuilderFactory, CatchupResponseHandler catchupResponseHandler,
            MultiDatabaseCatchupServerHandler serverResponseHandler ) throws Exception
    {
        if ( applicationProtocols == ApplicationProtocols.CATCHUP_3_0 )
        {
            new CatchupProtocolClientInstallerV3( pipelineBuilderFactory, emptyList(), LOG_PROVIDER, catchupResponseHandler,
                                                  new TestCommandReaderFactory() ).install( client );
            new CatchupProtocolServerInstallerV3( pipelineBuilderFactory, emptyList(), LOG_PROVIDER, serverResponseHandler, CatchupInboundEventListener.NO_OP )
                    .install( server );
        }
        else if ( applicationProtocols == ApplicationProtocols.CATCHUP_4_0 )
        {
            new CatchupProtocolClientInstallerV4( pipelineBuilderFactory, emptyList(), LOG_PROVIDER, catchupResponseHandler,
                                                  new TestCommandReaderFactory() ).install( client );
            new CatchupProtocolServerInstallerV4( pipelineBuilderFactory, emptyList(), LOG_PROVIDER, serverResponseHandler, CatchupInboundEventListener.NO_OP )
                    .install( server );
        }
        else if ( applicationProtocols == ApplicationProtocols.CATCHUP_5_0 )
        {
            new CatchupProtocolClientInstallerV5( pipelineBuilderFactory, emptyList(), LOG_PROVIDER, catchupResponseHandler,
                                                  new TestCommandReaderFactory() ).install( client );
            new CatchupProtocolServerInstallerV5( pipelineBuilderFactory, emptyList(), LOG_PROVIDER, serverResponseHandler, CatchupInboundEventListener.NO_OP )
                    .install( server );
        }
        else
        {
            throw new IllegalArgumentException( "Unsupported protocol " + applicationProtocols );
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
        public boolean onFileContent( FileChunk fileChunk )
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
        public void onGetDatabaseIdResponse( GetDatabaseIdResponse response )
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
        public void onGetAllDatabaseIdsResponse( GetAllDatabaseIdsResponse response )
        {
            unexpected();
        }

        @Override
        public void onInfo( InfoResponse msg )
        {
            unexpected();
        }

        @Override
        public void onGetMetadataResponse( GetMetadataResponse response )
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
