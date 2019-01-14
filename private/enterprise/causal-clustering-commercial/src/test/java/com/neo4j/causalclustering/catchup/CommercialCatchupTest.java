/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import io.netty.channel.embedded.EmbeddedChannel;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Function;

import org.neo4j.causalclustering.catchup.CatchupErrorResponse;
import org.neo4j.causalclustering.catchup.CatchupResponseHandler;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.FileChunk;
import org.neo4j.causalclustering.catchup.storecopy.FileHeader;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponse;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.causalclustering.catchup.tx.TxPullResponse;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import org.neo4j.causalclustering.catchup.v1.CatchupProtocolClientInstallerV1;
import org.neo4j.causalclustering.catchup.v1.CatchupProtocolServerInstallerV1;
import org.neo4j.causalclustering.catchup.v1.storecopy.GetStoreIdRequest;
import org.neo4j.causalclustering.catchup.v2.CatchupProtocolClientInstallerV2;
import org.neo4j.causalclustering.catchup.v2.CatchupProtocolServerInstallerV2;
import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.causalclustering.common.StubLocalDatabaseService;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.messaging.DatabaseCatchupRequest;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
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
    private StubLocalDatabaseService databaseService = new StubLocalDatabaseService();

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
    private CommercialCatchupServerHandler serverResponseHandler;

    @BeforeEach
    void setUp()
    {
        pipelineBuilderFactory = new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER );
        databaseService.givenDatabaseWithConfig().withDatabaseName( DEFAULT_DB ).register();
        serverResponseHandler = new CommercialCatchupServerHandler( databaseService, LOG_PROVIDER, fsa );
    }

    void executeTestScenario( Function<DatabaseService,RequestResponse> responseFunction ) throws Exception
    {
        RequestResponse requestResponse = responseFunction.apply( databaseService );

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

    static Function<DatabaseService,RequestResponse> storeId()
    {
        return databaseService ->

                new RequestResponse( new GetStoreIdRequest( DEFAULT_DB ), new ResponseAdaptor()
                {
                    @Override
                    public void onGetStoreIdResponse( GetStoreIdResponse response )
                    {
                        responseHandled = true;
                        assertEquals( databaseService.get( DEFAULT_DB ).map( LocalDatabase::storeId ).orElseThrow( IllegalStateException::new ),
                                response.storeId() );
                    }
                } );
    }

    static Function<DatabaseService,RequestResponse> wrongDb()
    {
        return databaseService ->

                new RequestResponse( new GetStoreIdRequest( "WRONG_DB_NAME" ), new ResponseAdaptor()
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
            CommercialCatchupServerHandler responseHandler ) throws Exception
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
