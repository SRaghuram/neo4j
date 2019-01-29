package com.neo4j.bench.micro.benchmarks.bolt;

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.logging.NullBoltMessageLogger;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.bolt.v1.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v1.runtime.BoltFactoryImpl;
import org.neo4j.bolt.v1.runtime.BoltResponseHandler;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.udc.UsageData;
import org.neo4j.values.AnyValue;

import static org.neo4j.bolt.v1.messaging.BoltResponseMessageWriter.NO_BOUNDARY_HOOK;

public abstract class AbstractBoltBenchmark extends BaseDatabaseBenchmark
{
    static final String USER_AGENT = "BoltPropertySerialization/0.0";

    static BoltFactoryImpl boltFactory( GraphDatabaseAPI db )
    {
        DependencyResolver resolver = db.getDependencyResolver();
        Authentication authentication = new BasicAuthentication( resolver.resolveDependency( AuthManager.class ),
                                                                 resolver.resolveDependency( UserManagerSupplier.class ) );
        return new BoltFactoryImpl(
                db,
                new UsageData( null ),
                NullLogService.getInstance(),
                resolver.resolveDependency( ThreadToStatementContextBridge.class ),
                authentication,
                BoltConnectionTracker.NOOP,
                Config.defaults()
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
    }

    static final BoltResponseHandler RESPONSE_HANDLER = new DummyBoltResultHandler();

    static class DummyBoltResultHandler implements BoltResponseHandler
    {
        private final PackedOutputArray out = new PackedOutputArray();
        private final BoltResponseMessageWriter writer = new BoltResponseMessageWriter( new Neo4jPack.Packer( out ),
                                                                                        NO_BOUNDARY_HOOK,
                                                                                        NullBoltMessageLogger.getInstance() );

        public byte[] result()
        {
            return out.bytes();
        }

        @Override
        public void onStart()
        {

        }

        public void reset()
        {
            out.reset();
        }

        @Override
        public void onRecords( BoltResult boltResult, boolean pull ) throws Exception
        {
            boltResult.accept( new BoltResult.Visitor()
            {
                @Override
                public void visit( QueryResult.Record record ) throws Exception
                {
                    writer.onRecord( record );
                    out.reset();
                }

                @Override
                public void addMetadata( String key, AnyValue value )
                {

                }
            } );
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
            return null;
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

    private static final ChannelHandlerContext CONTEXT = new ChannelHandlerContext()
    {
        @Override
        public Channel channel()
        {
            return CHANNEL;
        }

        @Override
        public EventExecutor executor()
        {
            return null;
        }

        @Override
        public String name()
        {
            return null;
        }

        @Override
        public ChannelHandler handler()
        {
            return null;
        }

        @Override
        public boolean isRemoved()
        {
            return false;
        }

        @Override
        public ChannelHandlerContext fireChannelRegistered()
        {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelUnregistered()
        {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelActive()
        {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelInactive()
        {
            return null;
        }

        @Override
        public ChannelHandlerContext fireExceptionCaught( Throwable throwable )
        {
            return null;
        }

        @Override
        public ChannelHandlerContext fireUserEventTriggered( Object o )
        {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelRead( Object o )
        {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelReadComplete()
        {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelWritabilityChanged()
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
        public ChannelHandlerContext read()
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
        public ChannelHandlerContext flush()
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
        public <T> Attribute<T> attr( AttributeKey<T> attributeKey )
        {
            return null;
        }

        @Override
        public <T> boolean hasAttr( AttributeKey<T> attributeKey )
        {
            return false;
        }
    };

    static final BoltChannel BOLT_CHANNEL = BoltChannel.open( CONTEXT, NullBoltMessageLogger.getInstance() );
}
