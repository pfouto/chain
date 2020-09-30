package ringpaxos.udppipeline;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MulticastExceptionHandler extends ChannelDuplexHandler {

    private static final Logger logger = LogManager.getLogger(MulticastExceptionHandler.class);

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        ctx.write(msg, promise.addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                Throwable cause = future.cause();
                cause.printStackTrace();
                logger.error("Multicast write error: " + future.cause());
                if (cause.getCause() instanceof AssertionError) {
                    cause.printStackTrace();
                    System.exit(1);
                }
                ctx.close();
            }
        }));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Multicast connection exception: " + ctx.channel().remoteAddress().toString() + " " + cause);
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        logger.debug("Multicast channel " + ctx.channel().remoteAddress() + " closed");
        ctx.fireChannelInactive();
    }
}
