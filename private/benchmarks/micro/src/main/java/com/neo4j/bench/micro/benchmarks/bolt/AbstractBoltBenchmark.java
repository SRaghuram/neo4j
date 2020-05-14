/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.bolt;

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.EventLoop;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltProtocolVersion;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.impl.BoltKernelDatabaseManagementServiceProvider;
import org.neo4j.bolt.packstream.Neo4jPackV1;
import org.neo4j.bolt.packstream.PackOutput;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.Neo4jError;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineFactory;
import org.neo4j.bolt.runtime.statemachine.impl.BoltStateMachineFactoryImpl;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.bolt.txtracking.DefaultReconciledTransactionTracker;
import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.bolt.v3.messaging.BoltResponseMessageWriterV3;
import org.neo4j.bolt.v4.BoltProtocolV4;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.AnyValue;

import static org.neo4j.bolt.transport.pipeline.ChannelProtector.NULL;

public abstract class AbstractBoltBenchmark extends BaseDatabaseBenchmark
{
    static final String USER_AGENT = "BoltPropertySerialization/0.0";
    public static final BoltProtocolVersion BOLT_VERSION = BoltProtocolV4.VERSION;

    static BoltStateMachineFactory boltFactory( GraphDatabaseAPI db )
    {
        DependencyResolver resolver = db.getDependencyResolver();
        DatabaseManagementService managementService = resolver.resolveDependency( DatabaseManagementService.class );
        Config config = resolver.resolveDependency( Config.class );
        Authentication authentication = new BasicAuthentication( resolver.resolveDependency( AuthManager.class ) );

        SystemNanoClock clock = Clocks.nanoClock();
        ReconciledTransactionTracker reconciledTxTracker = new DefaultReconciledTransactionTracker( NullLogService.getInstance() );
        BoltGraphDatabaseManagementServiceSPI databaseManagementService = new BoltKernelDatabaseManagementServiceProvider( managementService,
                reconciledTxTracker, new Monitors(), clock, config.get( GraphDatabaseSettings.bookmark_ready_timeout ) );
        return new BoltStateMachineFactoryImpl( databaseManagementService,
                authentication,
                clock,
                config,
                NullLogService.getInstance()
        );
    }

    static class PackedOutputArray implements PackOutput
    {
        ByteArrayOutputStream raw;
        DataOutputStream data;

        PackedOutputArray()
        {
            raw = new ByteArrayOutputStream();
            data = new DataOutputStream( raw );
        }

        public void reset()
        {
            raw = new ByteArrayOutputStream();
            data = new DataOutputStream( raw );
        }

        @Override
        public void beginMessage()
        {
            // do nothing
        }

        @Override
        public void messageSucceeded()
        {
            // do nothing
        }

        @Override
        public void messageFailed()
        {
            throw new RuntimeException( "Benchmark should never fail in this way" );
        }

        @Override
        public void messageReset()
        {
            throw new RuntimeException( "Benchmark should never fail in this way" );
        }

        @Override
        public PackOutput flush() throws IOException
        {
            data.flush();
            return this;
        }

        @Override
        public PackOutput writeByte( byte value ) throws IOException
        {
            data.write( value );
            return this;
        }

        @Override
        public PackOutput writeBytes( ByteBuffer buffer ) throws IOException
        {
            while ( buffer.remaining() > 0 )
            {
                data.writeByte( buffer.get() );
            }
            return this;
        }

        @Override
        public PackOutput writeBytes( byte[] bytes, int offset, int amountToWrite ) throws IOException
        {
            data.write( bytes, offset, amountToWrite );
            return this;
        }

        @Override
        public PackOutput writeShort( short value ) throws IOException
        {
            data.writeShort( value );
            return this;
        }

        @Override
        public PackOutput writeInt( int value ) throws IOException
        {
            data.writeInt( value );
            return this;
        }

        @Override
        public PackOutput writeLong( long value ) throws IOException
        {
            data.writeLong( value );
            return this;
        }

        @Override
        public PackOutput writeDouble( double value ) throws IOException
        {
            data.writeDouble( value );
            return this;
        }

        byte[] bytes()
        {
            return raw.toByteArray();
        }

        @Override
        public void close() throws IOException
        {
            data.close();
        }
    }

    static final BoltResponseHandler RESPONSE_HANDLER = new DummyBoltResultHandler();

    static class DummyBoltResultHandler implements BoltResponseHandler
    {
        private final PackedOutputArray out = new PackedOutputArray();
        private final BoltResponseMessageWriterV3 writer = new BoltResponseMessageWriterV3(
                new Neo4jPackV1(),
                out,
                NullLogService.getInstance() );

        public byte[] result()
        {
            return out.bytes();
        }

        public void reset()
        {
            out.reset();
        }

        @Override
        public boolean onPullRecords( BoltResult result, long size ) throws Throwable
        {
            return doOnRecords( result, size );
        }

        @Override
        public boolean onDiscardRecords( BoltResult result, long size )
        {
            throw new RuntimeException( "Did not expect this to happen in benchmarks" );
        }

        private boolean doOnRecords( BoltResult boltResult, long size ) throws Throwable
        {
            return boltResult.handleRecords(
                    new BoltResult.RecordConsumer()
                    {
                        @Override
                        public void beginRecord( int numberOfFields ) throws IOException
                        {
                            writer.beginRecord( numberOfFields );
                        }

                        @Override
                        public void consumeField( AnyValue value ) throws IOException
                        {
                            writer.consumeField( value );
                        }

                        @Override
                        public void endRecord() throws IOException
                        {
                            writer.endRecord();
                        }

                        @Override
                        public void onError()
                        {
                            writer.onError();
                        }

                        @Override
                        public void addMetadata( String key, AnyValue value )
                        {
                            //do nothing
                        }
                    },
                    size );
        }

        @Override
        public void onMetadata( String key, AnyValue value )
        {

        }

        @Override
        public void markIgnored()
        {

        }

        @Override
        public void markFailed( Neo4jError error )
        {

        }

        @Override
        public void onFinish()
        {

        }
    }

    @Override
    public String benchmarkGroup()
    {
        return "Bolt";
    }

    private static final Channel CHANNEL = new Channel()
    {
        @Override
        public ChannelId id()
        {
            return DefaultChannelId.newInstance();
        }

        @Override
        public EventLoop eventLoop()
        {
            return null;
        }

        @Override
        public Channel parent()
        {
            return null;
        }

        @Override
        public ChannelConfig config()
        {
            return null;
        }

        @Override
        public boolean isOpen()
        {
            return false;
        }

        @Override
        public boolean isRegistered()
        {
            return false;
        }

        @Override
        public boolean isActive()
        {
            return false;
        }

        @Override
        public ChannelMetadata metadata()
        {
            return null;
        }

        @Override
        public SocketAddress localAddress()
        {
            return null;
        }

        @Override
        public SocketAddress remoteAddress()
        {
            return null;
        }

        @Override
        public ChannelFuture closeFuture()
        {
            return null;
        }

        @Override
        public boolean isWritable()
        {
            return false;
        }

        @Override
        public long bytesBeforeUnwritable()
        {
            return 0;
        }

        @Override
        public long bytesBeforeWritable()
        {
            return 0;
        }

        @Override
        public Unsafe unsafe()
        {
            return null;
        }

        @Override
        public ChannelPipeline pipeline()
        {
            return null;
        }

        @Override
        public ByteBufAllocator alloc()
        {
            return null;
        }

        @Override
        public Channel read()
        {
            return null;
        }

        @Override
        public Channel flush()
        {
            return null;
        }

        @Override
        public ChannelFuture bind( SocketAddress socketAddress )
        {
            return null;
        }

        @Override
        public ChannelFuture connect( SocketAddress socketAddress )
        {
            return null;
        }

        @Override
        public ChannelFuture connect( SocketAddress socketAddress, SocketAddress socketAddress1 )
        {
            return null;
        }

        @Override
        public ChannelFuture disconnect()
        {
            return null;
        }

        @Override
        public ChannelFuture close()
        {
            return null;
        }

        @Override
        public ChannelFuture deregister()
        {
            return null;
        }

        @Override
        public ChannelFuture bind( SocketAddress socketAddress, ChannelPromise channelPromise )
        {
            return null;
        }

        @Override
        public ChannelFuture connect( SocketAddress socketAddress, ChannelPromise channelPromise )
        {
            return null;
        }

        @Override
        public ChannelFuture connect( SocketAddress socketAddress, SocketAddress socketAddress1, ChannelPromise channelPromise )
        {
            return null;
        }

        @Override
        public ChannelFuture disconnect( ChannelPromise channelPromise )
        {
            return null;
        }

        @Override
        public ChannelFuture close( ChannelPromise channelPromise )
        {
            return null;
        }

        @Override
        public ChannelFuture deregister( ChannelPromise channelPromise )
        {
            return null;
        }

        @Override
        public ChannelFuture write( Object o )
        {
            return null;
        }

        @Override
        public ChannelFuture write( Object o, ChannelPromise channelPromise )
        {
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush( Object o, ChannelPromise channelPromise )
        {
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush( Object o )
        {
            return null;
        }

        @Override
        public ChannelPromise newPromise()
        {
            return null;
        }

        @Override
        public ChannelProgressivePromise newProgressivePromise()
        {
            return null;
        }

        @Override
        public ChannelFuture newSucceededFuture()
        {
            return null;
        }

        @Override
        public ChannelFuture newFailedFuture( Throwable throwable )
        {
            return null;
        }

        @Override
        public ChannelPromise voidPromise()
        {
            return null;
        }

        @Override
        public <T> Attribute<T> attr( AttributeKey<T> attributeKey )
        {
            return null;
        }

        @Override
        public <T> boolean hasAttr( AttributeKey<T> attributeKey )
        {
            return false;
        }

        @Override
        public int compareTo( Channel o )
        {
            return 0;
        }
    };

    static final BoltChannel BOLT_CHANNEL = new BoltChannel( "bolt-1", "default", CHANNEL, NULL );
}
