package exchange.core2.core.utils;

import exchange.core2.core.processors.TwoStepSlaveProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RequiredArgsConstructor
public final class AffinityThreadFactory implements ThreadFactory {

    // There is a bug it LMAX Disruptor, when configuring dependency graph as processors, not handlers.
    // We have to track all threads requested from the factory to avoid duplicate reservations.
    private final Set<Object> affinityReservations = new HashSet<>();

    /**
     * 线程亲和性模式
     */
    private final ThreadAffinityMode threadAffinityMode;

    private static AtomicInteger threadsCounter = new AtomicInteger();

    /**
     * 创建新的线程
     *
     * @param runnable a runnable to be executed by new thread instance 一个由新线程执行的可执行程序
     *
     * @return Thread
     */
    @Override
    public synchronized Thread newThread(@NotNull Runnable runnable) {

        // log.info("---- Requesting thread for {}", runnable);

        if (threadAffinityMode == ThreadAffinityMode.THREAD_AFFINITY_DISABLE) {
            // 如果禁用亲和性模式则使用默认线程工厂创建线程
            return Executors.defaultThreadFactory().newThread(runnable);
        }

        if (runnable instanceof TwoStepSlaveProcessor) {
            log.debug("Skip pinning slave processor: {}", runnable);
            return Executors.defaultThreadFactory().newThread(runnable);
        }

        if (affinityReservations.contains(runnable)) {
            log.warn("Task {} was already pinned", runnable);
//            return Executors.defaultThreadFactory().newThread(runnable);
        }

        affinityReservations.add(runnable);

        return new Thread(() -> executePinned(runnable));

    }

    /**
     * 执行锁定操作
     *
     * 这个方法在获取线程亲和锁后执行传入的任务。
     * 它，然后获取并增加线程计数器，将当前线程命名为包含线程ID和CPU ID的格式。
     * 接着，它会记录任务将在哪个线程上运行，并调用runnable.run()执行任务。
     * 最后，在finally块中移除CPU锁和预留
     *
     * @param runnable 可执行任务
     */
    private void executePinned(@NotNull Runnable runnable) {

        try (final AffinityLock lock = getAffinityLockSync()) { // 首先获取一个亲和锁

            final int threadId = threadsCounter.incrementAndGet();
            Thread.currentThread().setName(String.format("Thread-AF-%d-cpu%d", threadId, lock.cpuId()));

            log.debug("{} will be running on thread={} pinned to cpu {}",
                    runnable, Thread.currentThread().getName(), lock.cpuId());

            runnable.run();

        } finally {
            log.debug("Removing cpu lock/reservation from {}", runnable);
            synchronized (this) {
                affinityReservations.remove(runnable);
            }
        }
    }

    /**
     * 用于获取线程亲和锁。
     * 根据threadAffinityMode的不同，会获取不同的亲和锁
     *
     * @return AffinityLock
     */
    private synchronized AffinityLock getAffinityLockSync() {
        return threadAffinityMode == ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE
                ? AffinityLock.acquireCore()
                : AffinityLock.acquireLock();
    }

    /**
     * 线程亲和性模式
     */
    public enum ThreadAffinityMode {
        /**
         * 每个物理核心启用
         */
        THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE,

        /**
         * 每个逻辑核心启用
         */
        THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE,

        /**
         * 禁用
         */
        THREAD_AFFINITY_DISABLE
    }

}
