package io.netty.util.concurrent;


import io.netty.util.concurrent.utils.ObjectUtil;

/**
 * from netty4.1
 */
public class FastThreadLocalRunnable implements Runnable {
    private Runnable runnable;

    public FastThreadLocalRunnable(Runnable runnable) {
        this.runnable = ObjectUtil.checkNotNull(runnable, "runnable");
    }

    public static Runnable wrap(Runnable runnable) {
        return runnable instanceof FastThreadLocalRunnable ? runnable : new FastThreadLocalRunnable(runnable);
    }

    @Override
    public void run() {
        try {
            // 运行任务
            this.runnable.run();
        } finally {
            /**
             * 线程池中的线程由于会被复用，所以线程池中的每一条线程在执行task结束后，要清理掉其InternalThreadLocalMap和其内的FastThreadLocal信息，
             * 否则，当这条线程在下一次被复用的时候，其ThreadLocalMap信息还存储着上一次被使用的时的信息；
             * 另外，假设这条线程不再被使用，但是这个线程有可能不会被销毁（与线程池的类型和配置相关），那么其上的ThreadLocal将发生了资源泄露。
             */
            FastThreadLocal.removeAll();
        }
    }
}
