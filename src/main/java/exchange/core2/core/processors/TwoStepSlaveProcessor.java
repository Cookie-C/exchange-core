/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.core.processors;

import com.lmax.disruptor.*;
import exchange.core2.core.common.CoreWaitStrategy;
import exchange.core2.core.common.cmd.OrderCommand;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * TwoStepSlaveProcessor
 *
 * TwoStepSlaveProcessor 是一个事件处理器，用于处理订单命令（OrderCommand）。
 * 它基于 LMAX Disruptor 框架实现，主要负责从环形缓冲区（RingBuffer）中读取订单命令，并调用相应的事件处理器进行处理。
 * 这个类的设计考虑了高性能、线程安全以及异常处理
 */
@Slf4j
public final class TwoStepSlaveProcessor implements EventProcessor {

    // Processor states 处理器状态

    /**
     * 闲置
     */
    private static final int IDLE = 0;

    /**
     * 停止
     */
    private static final int HALTED = IDLE + 1;

    /**
     * 运行
     */
    private static final int RUNNING = HALTED + 1;

    /**
     * Atomic integer to manage processor state 处理器状态
     */
    private final AtomicInteger running = new AtomicInteger(IDLE);

    /**
     * 用于从环形缓冲区获取OrderCommands的数据提供程序
     */
    private final DataProvider<OrderCommand> dataProvider;

    /**
     * 在处理器之间协调进程的序列屏障
     */
    private final SequenceBarrier sequenceBarrier;

    /**
     * 在无数据可用时等待策略的辅助工具
     */
    private final WaitSpinningHelper waitSpinningHelper;

    /**
     * 处理事件的处理程序
     */
    private final SimpleEventHandler eventHandler;

    /**
     * 序列跟踪环缓冲区中的当前位置
     */
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    /**
     * 用于在事件处理期间处理异常的异常处理程序
     */
    private final ExceptionHandler<? super OrderCommand> exceptionHandler;

    /**
     * 用于记录和调试目的的处理器名称
     */
    private final String name;

    /**
     * 下一个要处理的序列号
     */
    private long nextSequence = -1;

    /**
     * Constructor initializing the processor with necessary components.
     *
     * @param ringBuffer       RingBuffer containing OrderCommands.
     * @param sequenceBarrier  SequenceBarrier for coordination.
     * @param eventHandler     EventHandler to process commands.
     * @param exceptionHandler ExceptionHandler to handle errors.
     * @param name             Name of the processor.
     */
    public TwoStepSlaveProcessor(final RingBuffer<OrderCommand> ringBuffer,
                                 final SequenceBarrier sequenceBarrier,
                                 final SimpleEventHandler eventHandler,
                                 final ExceptionHandler<? super OrderCommand> exceptionHandler,
                                 final String name) {
        this.dataProvider = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
        this.waitSpinningHelper = new WaitSpinningHelper(ringBuffer, sequenceBarrier, 0, CoreWaitStrategy.SECOND_STEP_NO_WAIT);
        this.eventHandler = eventHandler;
        this.exceptionHandler = exceptionHandler;
        this.name = name;
    }

    @Override
    public Sequence getSequence() {
        return sequence;
    }

    /**
     * 停止处理器状态
      */
    @Override
    public void halt() {
        running.set(HALTED);
        sequenceBarrier.alert();
    }

    @Override
    public boolean isRunning() {
        return running.get() != IDLE;
    }

    /**
     * 启动处理器
     *
     * It is ok to have another thread rerun this method after a halt().
     * 让另一个线程在halt（）之后重新运行这个方法是可以的。
     *
     * @throws IllegalStateException if this object instance is already running in a thread
     */
    @Override
    public void run() {
        if (running.compareAndSet(IDLE, RUNNING)) {
            // 如果现在是闲置状态,则更新为运行状态
            sequenceBarrier.clearAlert();
        } else if (running.get() == RUNNING) {
            throw new IllegalStateException("Thread is already running (S)");
        }

        nextSequence = sequence.get() + 1L;
    }

    /**
     * 用于处理订单指令并按照指定顺序进行处理的主要流程。
     *
     * @param processUpToSequence 处理至目标序列
     */
    public void handlingCycle(final long processUpToSequence) {
        while (true) {
            OrderCommand event = null;
            try {
                long availableSequence = waitSpinningHelper.tryWaitFor(nextSequence);

                // process batch 批量处理命令
                while (nextSequence <= availableSequence && nextSequence < processUpToSequence) {
                    event = dataProvider.get(nextSequence);
                    eventHandler.onEvent(nextSequence, event); // TODO check if nextSequence is correct (not nextSequence+-1)?
                    nextSequence++;
                }

                // exit if finished processing entire group (up to specified sequence) Exit if finished processing entire group
                // 如果处理完整个组（不超过指定序列）如果处理完整个组退出
                if (nextSequence == processUpToSequence) {
                    sequence.set(processUpToSequence - 1);
                    waitSpinningHelper.signalAllWhenBlocking();
                    return;
                }
            } catch (final Throwable ex) {
                exceptionHandler.handleEventException(ex, nextSequence, event);
                sequence.set(nextSequence);
                waitSpinningHelper.signalAllWhenBlocking();
                nextSequence++;
            }
        }
    }

    /**
     * 返回处理器的字符串表示形式。
     *
      * @return String
     */
    @Override
    public String toString() {
        return "TwoStepSlaveProcessor{" + name + "}";
    }

}