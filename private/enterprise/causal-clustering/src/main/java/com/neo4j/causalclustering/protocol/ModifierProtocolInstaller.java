/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol;

import com.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.compression.JdkZlibDecoder;
import io.netty.handler.codec.compression.JdkZlibEncoder;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import io.netty.handler.codec.compression.Lz4FrameEncoder;
import io.netty.handler.codec.compression.SnappyFrameDecoder;
import io.netty.handler.codec.compression.SnappyFrameEncoder;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocols.COMPRESSION_GZIP;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocols.COMPRESSION_LZ4;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocols.COMPRESSION_LZ4_HIGH_COMPRESSION;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocols.COMPRESSION_LZ4_HIGH_COMPRESSION_VALIDATING;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocols.COMPRESSION_LZ4_VALIDATING;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocols.COMPRESSION_SNAPPY;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocols.COMPRESSION_SNAPPY_VALIDATING;
import static java.util.Arrays.asList;

public interface ModifierProtocolInstaller<O extends Orientation>
{
    Collection<ModifierProtocol> protocols();

    <BUILDER extends NettyPipelineBuilder<O,BUILDER>> void apply( NettyPipelineBuilder<O,BUILDER> nettyPipelineBuilder );

    List<ModifierProtocolInstaller<Orientation.Server>> serverCompressionInstallers =
            asList( new SnappyServer(), new SnappyValidatingServer(), new LZ4Server(), new LZ4ValidatingServer(), new GzipServer() );

    List<ModifierProtocolInstaller<Orientation.Client>> clientCompressionInstallers =
            asList( new SnappyClient(), new LZ4Client(), new LZ4HighCompressionClient(), new GzipClient() );

    List<ModifierProtocolInstaller<Orientation.Client>> allClientInstallers = clientCompressionInstallers;

    List<ModifierProtocolInstaller<Orientation.Server>> allServerInstallers = serverCompressionInstallers;

    abstract class BaseClientModifier implements ModifierProtocolInstaller<Orientation.Client>
    {
        private final String pipelineEncoderName;
        private final Supplier<MessageToByteEncoder<ByteBuf>> encoder;
        private final Collection<ModifierProtocol> modifierProtocols;

        protected BaseClientModifier( String pipelineEncoderName, Supplier<MessageToByteEncoder<ByteBuf>> encoder,
                ModifierProtocol... modifierProtocols )
        {
            this.pipelineEncoderName = pipelineEncoderName;
            this.encoder = encoder;
            this.modifierProtocols = asList( modifierProtocols );
        }

        @Override
        public final Collection<ModifierProtocol> protocols()
        {
            return modifierProtocols;
        }

        @Override
        public final <BUILDER extends NettyPipelineBuilder<Orientation.Client,BUILDER>> void apply(
                NettyPipelineBuilder<Orientation.Client,BUILDER> nettyPipelineBuilder )
        {
            nettyPipelineBuilder.add( pipelineEncoderName, encoder.get() );
        }
    }

    abstract class BaseServerModifier implements ModifierProtocolInstaller<Orientation.Server>
    {
        private final String pipelineDecoderName;
        private final Supplier<ByteToMessageDecoder> decoder;
        private final Collection<ModifierProtocol> modifierProtocols;

        protected BaseServerModifier( String pipelineDecoderName, Supplier<ByteToMessageDecoder> decoder, ModifierProtocol... modifierProtocols )
        {
            this.pipelineDecoderName = pipelineDecoderName;
            this.decoder = decoder;
            this.modifierProtocols = asList( modifierProtocols );
        }

        @Override
        public final Collection<ModifierProtocol> protocols()
        {
            return modifierProtocols;
        }

        @Override
        public final <BUILDER extends NettyPipelineBuilder<Orientation.Server,BUILDER>> void apply(
                NettyPipelineBuilder<Orientation.Server,BUILDER> nettyPipelineBuilder )
        {
            nettyPipelineBuilder.add( pipelineDecoderName, decoder.get() );
        }
    }

    class SnappyClient extends BaseClientModifier
    {
        SnappyClient()
        {
            super( "snappy_encoder", SnappyFrameEncoder::new, COMPRESSION_SNAPPY, COMPRESSION_SNAPPY_VALIDATING );
        }
    }

    class SnappyServer extends BaseServerModifier
    {
        SnappyServer()
        {
            super( "snappy_decoder", SnappyFrameDecoder::new, COMPRESSION_SNAPPY );
        }
    }

    class SnappyValidatingServer extends BaseServerModifier
    {
        SnappyValidatingServer()
        {
            super( "snappy_validating_decoder", () -> new SnappyFrameDecoder( true ), COMPRESSION_SNAPPY_VALIDATING );
        }
    }

    class LZ4Client extends BaseClientModifier
    {
        LZ4Client()
        {
            super( "lz4_encoder", Lz4FrameEncoder::new, COMPRESSION_LZ4, COMPRESSION_LZ4_VALIDATING );
        }
    }

    class LZ4HighCompressionClient extends BaseClientModifier
    {
        LZ4HighCompressionClient()
        {
            super( "lz4_encoder", () -> new Lz4FrameEncoder( true ),
                    COMPRESSION_LZ4_HIGH_COMPRESSION, COMPRESSION_LZ4_HIGH_COMPRESSION_VALIDATING );
        }
    }

    class LZ4Server extends BaseServerModifier
    {
        LZ4Server()
        {
            super( "lz4_decoder", Lz4FrameDecoder::new, COMPRESSION_LZ4, COMPRESSION_LZ4_HIGH_COMPRESSION );
        }
    }

    class LZ4ValidatingServer extends BaseServerModifier
    {
        LZ4ValidatingServer()
        {
            super( "lz4_decoder", () -> new Lz4FrameDecoder( true ),
                    COMPRESSION_LZ4_VALIDATING, COMPRESSION_LZ4_HIGH_COMPRESSION_VALIDATING );
        }
    }

    class GzipClient extends BaseClientModifier
    {
        GzipClient()
        {
            super( "zlib_encoder", JdkZlibEncoder::new, COMPRESSION_GZIP );
        }
    }

    class GzipServer extends BaseServerModifier
    {
        GzipServer()
        {
            super( "zlib_decoder", JdkZlibDecoder::new, COMPRESSION_GZIP );
        }
    }
}
