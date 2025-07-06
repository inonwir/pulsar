package io.netty.example.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

/**
 * A Netty proxy frontend handler that forwards incoming traffic
 * to two backend servers (Server 2 and Server 3).
 * Server 2 can send and receive traffic,
 * while Server 3 only receives traffic — any replies are discarded.
 */
public class HexDumpProxyFrontendHandler extends ChannelInboundHandlerAdapter {

    private final String remoteHost;
    private final int server2Port;
    private final int server3Port;

    private Channel server2OutboundChannel;
    private Channel server3OutboundChannel;

    private final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    public HexDumpProxyFrontendHandler(String remoteHost, int server2Port, int server3Port) {
        this.remoteHost = remoteHost;
        this.server2Port = server2Port;
        this.server3Port = server3Port;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        final Channel inboundChannel = ctx.channel();

        // Connect to Server 3 (only to send data, replies discarded)
        Bootstrap server3Bootstrap = new Bootstrap();
        server3Bootstrap.group(inboundChannel.eventLoop())
                .channel(ctx.channel().getClass())
                .handler(new DiscardServerHandler())
                .option(ChannelOption.AUTO_READ, false);

        ChannelFuture server3Future = server3Bootstrap.connect(remoteHost, server3Port);
        server3OutboundChannel = server3Future.channel();

        // Connect to Server 2 (full duplex)
        Bootstrap server2Bootstrap = new Bootstrap();
        server2Bootstrap.group(inboundChannel.eventLoop())
                .channel(ctx.channel().getClass())
                .handler(new HexDumpProxyBackendHandler(inboundChannel))
                .option(ChannelOption.AUTO_READ, false);

        ChannelFuture server2Future = server2Bootstrap.connect(remoteHost, server2Port);
        server2OutboundChannel = server2Future.channel();

        server2Future.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                inboundChannel.read();
            } else {
                inboundChannel.close();
            }
        });

        // Add channels to the group
        channels.add(server2OutboundChannel);
        channels.add(server3OutboundChannel);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        msg.retain(); // increment ref count because we send to two channels

        if (server2OutboundChannel.isActive()) {
            server2OutboundChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    ctx.channel().read();
                } else {
                    future.channel().close();
                }
            });
        }

        if (server3OutboundChannel.isActive()) {
            server3OutboundChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    ctx.channel().read();
                } else {
                    future.channel().close();
                }
            });
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (server2OutboundChannel != null) {
            closeOnFlush(server2OutboundChannel);
        }
        if (server3OutboundChannel != null) {
            closeOnFlush(server3OutboundChannel);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        closeOnFlush(ctx.channel());
    }

    /**
     * Closes the specified channel after all queued write requests are flushed.
     */
    static void closeOnFlush(Channel ch) {
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
