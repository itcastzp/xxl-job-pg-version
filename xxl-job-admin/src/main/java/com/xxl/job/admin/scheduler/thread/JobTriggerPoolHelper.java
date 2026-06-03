package com.xxl.job.admin.scheduler.thread;

import com.xxl.job.admin.scheduler.config.XxlJobAdminBootstrap;
import com.xxl.job.admin.scheduler.trigger.TriggerTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * job trigger thread pool helper
 *
 * @author xuxueli 2018-07-03 21:08:07
 */
public class JobTriggerPoolHelper {
    private static Logger logger = LoggerFactory.getLogger(JobTriggerPoolHelper.class);


    // ---------------------- trigger pool ----------------------

    // 快慢线程池隔离设计：处理正常任务与超时任务
    private ThreadPoolExecutor fastTriggerPool = null;
    private ThreadPoolExecutor slowTriggerPool = null;

    /**
     * start
     */
    public void start() {
        fastTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminBootstrap.getInstance().getTriggerPoolFastMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode());
                    }
                },
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        logger.error(">>>>>>>>>>> xxl-job, admin JobTriggerPoolHelper-fastTriggerPool execute too fast, Runnable=" + r.toString());
                    }
                });

        slowTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminBootstrap.getInstance().getTriggerPoolSlowMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(5000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode());
                    }
                },
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        logger.error(">>>>>>>>>>> xxl-job, admin JobTriggerPoolHelper-slowTriggerPool execute too fast, Runnable=" + r.toString());
                    }
                });
    }

    /**
     * stop
     */
    public void stop() {
        //triggerPool.shutdown();
        fastTriggerPool.shutdownNow();
        slowTriggerPool.shutdownNow();
        logger.info(">>>>>>>>> xxl-job trigger thread pool shutdown success.");
    }


    // job timeout count
    private volatile long minTim = System.currentTimeMillis() / 60000;     // ms > min
    private volatile ConcurrentMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();


    // ---------------------- tool ----------------------

    /**
     * trigger job
     *
     * @param jobId
     * @param triggerType
     * @param failRetryCount        >=0: use this param
     *                              <0: use param from job info config
     * @param executorShardingParam
     * @param executorParam         null: use job param
     *                              not null: cover job param
     */
    public void trigger(final int jobId,
                        final TriggerTypeEnum triggerType,
                        final int failRetryCount,
                        final String executorShardingParam,
                        final String executorParam,
                        final String addressList) {

        // 1. 线程池选择逻辑 (隔离核心)
        ThreadPoolExecutor triggerPool_ = fastTriggerPool;
        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);

        // 关键判定：若任务在 1 分钟内连续超时 (>500ms) 超过 10 次，则降级到“慢池”执行
        if (jobTimeoutCount != null && jobTimeoutCount.get() > 10) {      // job-timeout 10 times in 1 min
            triggerPool_ = slowTriggerPool;
        }

        // trigger
        triggerPool_.execute(new Runnable() {
            @Override
            public void run() {

                long start = System.currentTimeMillis();

                try {
                    // do trigger
                    XxlJobAdminBootstrap.getInstance().getJobTrigger().trigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                } finally {

                    // 2. 超时统计维护：按分钟清理计数器
                    long minTim_now = System.currentTimeMillis() / 60000;
                    if (minTim != minTim_now) {
                        minTim = minTim_now;
                        jobTimeoutCountMap.clear();
                    }

                    // 耗时判定：若本次触发调用耗时超过 500ms，则计为一次超时
                    long cost = System.currentTimeMillis() - start;
                    if (cost > 500) {       // ob-timeout threshold 500ms
                        AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                        if (timeoutCount != null) {
                            timeoutCount.incrementAndGet();
                        }
                    }

                }

            }

            @Override
            public String toString() {
                return "Job Runnable, jobId:" + jobId;
            }
        });
    }

}
