package com.auto.demo.redis.client.handler;

import com.auto.demo.redis.core.output.CommandOutput;
import com.auto.demo.redis.core.protocol.RedisCommand;
import com.auto.demo.redis.core.protocol.RedisStateMachine;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * @author wbs
 * @date 2020/10/20
 */
public class RedisClientHandler extends ChannelDuplexHandler {

    private final ArrayDeque<RedisCommand<?, ?, ?>> stack = new ArrayDeque<>();

    private final float discardReadBytesRatio = 0.8f;

    private final RedisStateMachine rsm = new RedisStateMachine();

    private ByteBuf buffer;

    Channel channel;

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {

        channel = ctx.channel();

        buffer = ctx.alloc().buffer(8192 * 8);
        ctx.fireChannelRegistered();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {

        if (channel != null && ctx.channel() != channel) {
            ctx.fireChannelUnregistered();
            return;
        }

        channel = null;
        buffer.release();

        reset();

        rsm.close();

        ctx.fireChannelUnregistered();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof RedisCommand) {
            writeSingleCommand(ctx, (RedisCommand<?, ?, ?>) msg, promise);
            return;
        }
    }

    private void writeSingleCommand(ChannelHandlerContext ctx, RedisCommand<?, ?, ?> command, ChannelPromise promise) {

        if (!isWriteable(command)) {
            promise.trySuccess();
            return;
        }

        addToStack(command, promise);

        ctx.write(command, promise);
    }

    private void addToStack(RedisCommand<?, ?, ?> command, ChannelPromise promise) {

        try {

            if (promise.isVoid()) {
                stack.add(command);
            }
        } catch (Exception e) {
            command.completeExceptionally(e);
            throw e;
        }
    }

    private static boolean isWriteable(RedisCommand<?, ?, ?> command) {
        return !command.isDone();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf input = (ByteBuf) msg;
        if (!input.isReadable() || input.refCnt() == 0) {
            return;
        }
        try {

            buffer.touch("CommandHandler.read(…)");
            buffer.writeBytes(input);

            decode(ctx, buffer);
        } finally {
            input.release();
        }

    }

    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer) throws InterruptedException {


        while (canDecode(buffer)) {

            RedisCommand<?, ?, ?> command = stack.peek();

            try {
                if (!decode(ctx, buffer, command)) {
                    discardReadBytesIfNecessary(buffer);
                    return;
                }
            } catch (Exception e) {

                ctx.close();
                throw e;
            }

            if (canComplete(command)) {
                stack.poll();

                try {
                    complete(command);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            afterDecode(ctx, command);
        }

        discardReadBytesIfNecessary(buffer);
    }

    protected boolean canDecode(ByteBuf buffer) {
        return !stack.isEmpty() && buffer.isReadable();
    }

    protected boolean canComplete(RedisCommand<?, ?, ?> command) {
        return true;
    }

    protected void complete(RedisCommand<?, ?, ?> command) {
        command.complete();
    }

    protected void afterDecode(ChannelHandlerContext ctx, RedisCommand<?, ?, ?> command) {
    }

    private boolean decode(ChannelHandlerContext ctx, ByteBuf buffer, RedisCommand<?, ?, ?> command) {
        return decode0(ctx, buffer, command);
    }

    private boolean decode0(ChannelHandlerContext ctx, ByteBuf buffer, RedisCommand<?, ?, ?> command) {

        if (!decode(buffer, command, getCommandOutput(command))) {
            return false;
        }

        return true;
    }

    protected CommandOutput<?, ?, ?> getCommandOutput(RedisCommand<?, ?, ?> command) {
        return command.getOutput();
    }

    protected boolean decode(ByteBuf buffer, CommandOutput<?, ?, ?> output) {
        return rsm.decode(buffer, output);
    }

    protected boolean decode(ByteBuf buffer, RedisCommand<?, ?, ?> command, CommandOutput<?, ?, ?> output) {
        return rsm.decode(buffer, command, output);
    }


    private void discardReadBytesIfNecessary(ByteBuf buffer) {

        float usedRatio = (float) buffer.readerIndex() / buffer.capacity();

        if (usedRatio >= discardReadBytesRatio && buffer.refCnt() != 0) {
            buffer.discardReadBytes();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.print("exceptionCaught: ");
        cause.printStackTrace(System.err);
        ctx.close();
    }

    private void reset() {

        resetInternals();
        cancelCommands("Reset", drainCommands(stack));
    }

    private void resetInternals() {

        rsm.reset();

        if (buffer.refCnt() > 0) {
            buffer.clear();
        }
    }

    private static void cancelCommands(String message, List<RedisCommand<?, ?, ?>> toCancel) {

        for (RedisCommand<?, ?, ?> cmd : toCancel) {
            if (cmd.getOutput() != null) {
                cmd.getOutput().setError(message);
            }
            cmd.cancel();
        }
    }

    private static <T> List<T> drainCommands(Queue<T> source) {

        List<T> target = new ArrayList<>(source.size());

        T cmd;
        while ((cmd = source.poll()) != null) {
            target.add(cmd);
        }

        return target;
    }

}
