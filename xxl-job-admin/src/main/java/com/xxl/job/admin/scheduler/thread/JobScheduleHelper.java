package com.xxl.job.admin.scheduler.thread;

import com.xxl.job.admin.constant.TriggerStatus;
import com.xxl.job.admin.model.XxlJobInfo;
import com.xxl.job.admin.scheduler.config.XxlJobAdminBootstrap;
import com.xxl.job.admin.scheduler.misfire.MisfireStrategyEnum;
import com.xxl.job.admin.scheduler.type.ScheduleTypeEnum;
import com.xxl.job.admin.scheduler.trigger.TriggerTypeEnum;
import com.xxl.tool.core.CollectionTool;
import com.xxl.tool.core.MapTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 *  * 一个任务的完整流程
 *  * 1. 注册与发现 (Heartbeat)
 *  * 执行器启动后，会定时（默认 30s）向调度中心发送“心跳”，告诉 Admin：“我还在，这是我的 IP 和端口”。
 *  * Admin 会把这些信息存在数据库里，形成一个“动态地址列表”。
 *  * 2. 调度触发 (Trigger)
 *  * Admin 内部有一个定时线程池。当任务的 Cron 时间到了，Admin 会根据配置的“路由策略”（如轮询、随机、一致性 Hash）从地址列表中选出一个执行器。
 *  * 3. 指令下发 (Dispatch)
 *  * Admin 调用执行器的接口（/run），把任务参数、LogId、处理逻辑名称发过去。
 *  * 4. 任务执行 (Execution)
 *  * 执行器接收到请求后，会启动一个 JobThread 来运行业务代码。
 *  * 注意：执行器是异步执行的，它会立刻给 Admin 返回“已收到请求”，然后慢慢跑业务逻辑。
 *  * 5. 回报结果 (Callback)
 *  * 业务跑完后（成功或失败），执行器会将结果放入一个“回调队列”，由专门的线程批量异步回传给 Admin 的接口（/callback）。
 * @author xuxueli 2019-05-21
 */
public class JobScheduleHelper {
    private static final Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);


    /**
     * 调度预读时间阈值：5000 毫秒
     * 调度中心会提前 5 秒扫描数据库，将即将触发的任务捞取到内存或时间轮中
     */
    public static final long PRE_READ_MS = 5000;    // pre read

    private Thread scheduleThread;
    private Thread ringThread;
    private volatile boolean scheduleThreadToStop = false;
    private volatile boolean ringThreadToStop = false;
    private final Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();

    /**
     * start
     */
    public void start(){

        // schedule thread
        scheduleThread = new Thread(new Runnable() {
            @Override
            public void run() {

                // 1. 时间对齐：启动时对齐到秒刻度，保证调度精确度
                try {
                    TimeUnit.MILLISECONDS.sleep(5000 - System.currentTimeMillis()%1000 );
                } catch (Throwable e) {
                    if (!scheduleThreadToStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info(">>>>>>>>> init xxl-job admin scheduler success.");

                // pre-read count: treadpool-size * trigger-qps (each trigger cost 100ms, qps = 1000/100 = 100)
                int preReadCount = (XxlJobAdminBootstrap.getInstance().getTriggerPoolFastMax() + XxlJobAdminBootstrap.getInstance().getTriggerPoolSlowMax()) * 10;

                // 2. 调度主循环
                while (!scheduleThreadToStop) {

                    // 记录起始时间
                    long start = System.currentTimeMillis();
                    boolean preReadSuc = true;

                    // transaction start
                    TransactionStatus transactionStatus = XxlJobAdminBootstrap.getInstance().getTransactionManager().getTransaction(new DefaultTransactionDefinition());
                    try {
                        // 2.1 获取分布式调度锁：利用 DB 行锁 (select for update) 确保集群环境下只有一个节点扫描 DB
                        String lockedRecord = XxlJobAdminBootstrap.getInstance().getXxlJobLockMapper().scheduleLock();
                        long nowTime = System.currentTimeMillis();

                        // 2.2 查询未来 5s 内即将触发的任务列表
                        List<XxlJobInfo> scheduleList = XxlJobAdminBootstrap.getInstance().getXxlJobInfoMapper().scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);
                        if (CollectionTool.isNotEmpty(scheduleList)) {

                            // 2.3 分类处理捞取到的任务
                            for (XxlJobInfo jobInfo: scheduleList) {

                                // 任务过期判定
                                if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS) {
                                    // 场景 A：严重过期 (>5s)。
                                    // 策略：根据 MisfireStrategy 处理（忽略或补偿一次），并重新计算下次时间

                                    // 1、misfire handle
                                    MisfireStrategyEnum misfireStrategyEnum = MisfireStrategyEnum.match(jobInfo.getMisfireStrategy(), MisfireStrategyEnum.DO_NOTHING);
                                    misfireStrategyEnum.getMisfireHandler().handle(jobInfo.getId());

                                    // 2、fresh next
                                    refreshNextTriggerTime(jobInfo, new Date());

                                } else if (nowTime > jobInfo.getTriggerNextTime()) {
                                    // 场景 B：轻微过期 (<5s)。
                                    // 策略：立即触发一次，并重新计算下次时间

                                    // 1、trigger direct
                                    XxlJobAdminBootstrap.getInstance().getJobTriggerPoolHelper().trigger(jobInfo.getId(), TriggerTypeEnum.CRON, -1, null, null, null);
                                    logger.debug(">>>>>>>>>>> xxl-job, schedule expire, direct trigger : jobId = " + jobInfo.getId() );

                                    // 2、fresh next
                                    refreshNextTriggerTime(jobInfo, new Date());

                                    // next-trigger-time in 5s, pre-read again
                                    if (jobInfo.getTriggerStatus()== TriggerStatus.RUNNING.getValue() && nowTime + PRE_READ_MS > jobInfo.getTriggerNextTime()) {

                                        // 1、make ring second
                                        int ringSecond = (int)((jobInfo.getTriggerNextTime()/1000)%60);

                                        // 2、push time ring (pre read)
                                        pushTimeRing(ringSecond, jobInfo.getId());
                                        logger.debug(">>>>>>>>>>> xxl-job, schedule pre-read, push trigger : jobId = " + jobInfo.getId() );

                                        // 3、fresh next
                                        refreshNextTriggerTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));

                                    }

                                } else {
                                    // 场景 C：正常预读 (触发时间在未来 5s 内)
                                    // 策略：计算刻度并塞入“时间轮 (Time-Ring)”

                                    // 1、计算任务在时间轮中的秒数刻度 (0-59秒)
                                    int ringSecond = (int)((jobInfo.getTriggerNextTime()/1000)%60);

                                    // 2、塞入内存时间轮
                                    pushTimeRing(ringSecond, jobInfo.getId());
                                    logger.debug(">>>>>>>>>>> xxl-job, schedule normal, push trigger : jobId = " + jobInfo.getId() );

                                    // 3、fresh next
                                    refreshNextTriggerTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));

                                }

                            }

                            // 3、update trigger info
                            for (XxlJobInfo jobInfo: scheduleList) {
                                XxlJobAdminBootstrap.getInstance().getXxlJobInfoMapper().scheduleUpdate(jobInfo);
                            }

                        } else {
                            preReadSuc = false;
                        }

                    } catch (Throwable e) {
                        if (!scheduleThreadToStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread error:{}", e.getMessage(), e);
                        }
                    } finally {
                        // transaction commit
                        XxlJobAdminBootstrap.getInstance().getTransactionManager().commit(transactionStatus);   // avlid schedule repeat
                    }
                    // 2.4 计算耗时并执行动态休眠策略
                    long cost = System.currentTimeMillis()-start;


                    // 休眠对齐：若读到任务则每秒扫描，没读到则跳过本预读周期
                    if (cost < 1000) {  // scan-overtime, not wait
                        try {
                            // pre-read period: success > scan each second; fail > skip this period;
                            TimeUnit.MILLISECONDS.sleep((preReadSuc?1000:PRE_READ_MS) - System.currentTimeMillis()%1000);
                        } catch (Throwable e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                    }

                }

                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread stop");
            }
        });
        scheduleThread.setDaemon(true);
        scheduleThread.setName("xxl-job, admin JobScheduleHelper#scheduleThread");
        scheduleThread.start();


        // ring thread
        ringThread = new Thread(new Runnable() {
            @Override
            public void run() {

                while (!ringThreadToStop) {

                    // 3. 时间轮线程：每秒 tick 一次，触发内存任务
                    try {
                        // 3.1 对齐秒刻度运行
                        TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis() % 1000);
                    } catch (Throwable e) {
                        if (!ringThreadToStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }

                    try {
                        // second data
                        List<Integer> ringItemData = new ArrayList<>();

                        // 3.2 获取当前刻度的任务数据
                        int nowSecond = Calendar.getInstance().get(Calendar.SECOND);
                        for (int i = 0; i <= 2; i++) {                                                              // 补偿机制：多检查前 2 秒的数据，防止调度遗漏
                            List<Integer> ringItemList = ringData.remove( (nowSecond+60-i)%60 );
                            if (CollectionTool.isNotEmpty(ringItemList)) {
                                // distinct for each second
                                List<Integer> ringItemListDistinct = ringItemList.stream().distinct().toList();     // 避免调度重复：重复推送时间轮刻度，去重只保留一个；；
                                if (ringItemListDistinct.size() < ringItemList.size()) {
                                    logger.warn(">>>>>>>>>>> xxl-job, time-ring found job repeat beat : " + nowSecond + " = " + ringItemData);
                                }

                                // collect ring item
                                ringItemData.addAll(ringItemListDistinct);
                            }
                        }

                        // ring trigger
                        logger.debug(">>>>>>>>>>> xxl-job, time-ring beat : " + nowSecond + " = " + ringItemData);
                        if (CollectionTool.isNotEmpty(ringItemData)) {
                            // do trigger
                            for (int jobId: ringItemData) {
                                // do trigger
                                XxlJobAdminBootstrap.getInstance().getJobTriggerPoolHelper().trigger(jobId, TriggerTypeEnum.CRON, -1, null, null, null);
                            }
                            // clear
                            ringItemData.clear();
                        }
                    } catch (Throwable e) {
                        if (!ringThreadToStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread error:{}", e.getMessage(), e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread stop");
            }
        });
        ringThread.setDaemon(true);
        ringThread.setName("xxl-job, admin JobScheduleHelper#ringThread");
        ringThread.start();
    }

    /**
     * refresh next trigger time of job
     *
     * @param jobInfo   job info
     * @param fromTime  from time
     */
    private void refreshNextTriggerTime(XxlJobInfo jobInfo, Date fromTime) {
        try {
            // generate next trigger time
            ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), ScheduleTypeEnum.NONE);
            Date nextTriggerTime = scheduleTypeEnum.getScheduleType().generateNextTriggerTime(jobInfo, fromTime);

            // refresh next trigger-time + status
            if (nextTriggerTime != null) {
                // generate success
                jobInfo.setTriggerStatus(-1);                               // pass, may be Inaccurate
                jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
                jobInfo.setTriggerNextTime(nextTriggerTime.getTime());
            } else {
                // generate fail, stop job
                jobInfo.setTriggerStatus(TriggerStatus.STOPPED.getValue());
                jobInfo.setTriggerLastTime(0);
                jobInfo.setTriggerNextTime(0);
                logger.error(">>>>>>>>>>> xxl-job, refreshNextValidTime fail for job: jobId={}, scheduleType={}, scheduleConf={}",
                        jobInfo.getId(), jobInfo.getScheduleType(), jobInfo.getScheduleConf());
            }
        } catch (Throwable e) {
            // generate error, stop job
            jobInfo.setTriggerStatus(TriggerStatus.STOPPED.getValue());
            jobInfo.setTriggerLastTime(0);
            jobInfo.setTriggerNextTime(0);

            logger.error(">>>>>>>>>>> xxl-job, refreshNextValidTime error for job: jobId={}, scheduleType={}, scheduleConf={}",
                    jobInfo.getId(), jobInfo.getScheduleType(), jobInfo.getScheduleConf(), e);
        }
    }

    /**
     * push time ring
     *
     * @param ringSecond    ring second
     * @param jobId         job id
     */
    private void pushTimeRing(int ringSecond, int jobId){
        // get ringItemData, init when not exists
        List<Integer> ringItemList = ringData.computeIfAbsent(
                ringSecond,
                k -> new ArrayList<>());

        // push async rind
        ringItemList.add(jobId);
        logger.debug(">>>>>>>>>>> xxl-job, schedule push time-ring : " + ringSecond + " = " + List.of(ringItemList));
    }

    /**
     * 优雅停止调度引擎
     */
    public void stop(){

        // 1、停止主扫描线程
        scheduleThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);  // wait
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        if (scheduleThread.getState() != Thread.State.TERMINATED){
            // 中断并等待
            scheduleThread.interrupt();
            try {
                scheduleThread.join();
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
            }
        }

        // if has ring data
        boolean hasRingData = false;
        if (MapTool.isNotEmpty(ringData)) {
            for (int second : ringData.keySet()) {
                List<Integer> ringItemList = ringData.get(second);
                if (CollectionTool.isNotEmpty(ringItemList)) {
                    hasRingData = true;
                    break;
                }
            }
        }
        if (hasRingData) {
            try {
                TimeUnit.SECONDS.sleep(8);
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
            }
        }

        // stop ring (wait job-in-memory stop)
        ringThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        if (ringThread.getState() != Thread.State.TERMINATED){
            // interrupt and wait
            ringThread.interrupt();
            try {
                ringThread.join();
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
            }
        }

        logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper stop");
    }

}
