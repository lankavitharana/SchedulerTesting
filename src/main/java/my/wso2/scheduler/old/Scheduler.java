package my.wso2.scheduler.old;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class Scheduler {

    private int numThreads;
    public Semaphore mainBlockSem;
    private BlockingQueue<SchedulerItem> commonQueue;
    private AtomicInteger totalStrands = new AtomicInteger();

    public boolean immortal = true;


    public Semaphore executionLock;

    public Scheduler(int numThreads, Semaphore executionLock) {
        this.numThreads = numThreads;
        this.commonQueue = new LinkedBlockingDeque<>();
        this.executionLock = executionLock;
    }

    public void start() {
        this.mainBlockSem = new Semaphore(-(numThreads - 1));
        for (int i = 0; i < numThreads - 1; i++) {
            new Thread(this::runTask, "jbal-strand-exec-" + i).start();
        }

        try {
            runTask();
        } catch (Throwable t) {
            t.printStackTrace();
        }

        try {
            this.mainBlockSem.acquire();
            this.executionLock.release();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void schedule(SchedulerItem item) {
        this.totalStrands.incrementAndGet();
        this.commonQueue.add(item);
    }

    public void runTask() {
        int count = 0;
        while (true) {
            SchedulerItem item;
            try {
                item = this.commonQueue.take();
            } catch (InterruptedException e) {
                continue;
            }
            if (item.poison) {
                this.mainBlockSem.release();
                break;
            }

            item.function.apply(item.params);
            count++;
            int strandsLeft = totalStrands.decrementAndGet();
            if (strandsLeft == 0 && !immortal) {
                // (number of started stands - finished stands) = 0, all the work is done
                for (int i = 0; i < numThreads; i++) {
                    this.commonQueue.add(new SchedulerItem());
                }
            }
        }
        System.out.println("Thread id - " + Thread.currentThread().getId() + " count - " + count);
    }

}

class SchedulerItem {
    public Function<Object[], Object> function;
    boolean poison;
    public Object[] params;

    public SchedulerItem() {
        this.poison = true;
    }

    public SchedulerItem(Function<Object[], Object> function, Object[] params) {
        this.function = function;
        this.params = params;
    }
}


class SchedulerTest {
    static AtomicInteger total = new AtomicInteger(0);

    public static void main(String[] args) {

        int threadCount = 8;
        int submitterLimit = 5;
        int itemPerSubmitterLimit = 1000000;
        int loopCountInsideFunction = 1000;



        Semaphore executionLock = new Semaphore(0);
        ExecutorService workerExecutor = Executors.newFixedThreadPool(submitterLimit);
        Scheduler old = new Scheduler(threadCount, executionLock);
        AtomicInteger submitterCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();
        for (int j = 0; j < submitterLimit; j++) {
            workerExecutor.submit(() -> {
                for (int i = 0; i < itemPerSubmitterLimit; i++) {
                    Function<Object[], Object> func = SchedulerTest::apply;
                    SchedulerItem item = new SchedulerItem(func, new Object[]{loopCountInsideFunction, "item " + i});
                    old.schedule(item);
                }
                if (submitterCount.incrementAndGet() == submitterLimit) {
                    // this is to finish the execution of the scheduler
                    old.immortal = false;
                    Function<Object[], Object> func = SchedulerTest::apply;
                    SchedulerItem item = new SchedulerItem(func, new Object[]{1, "final "});
                    old.schedule(item);
                }
            });
        }

        old.start();

        long endTime = System.currentTimeMillis();
        int totalSubmitted = 1 + (submitterLimit * itemPerSubmitterLimit);
        String result = "----- Results -----\n";
        result += "total workers   - " + threadCount + "\n";
        result += "total submitted - " + totalSubmitted + "\n";
        result += "total executed  - " + total.get() + "\n";
        result += "total time      - " + (endTime - startTime) + "ms\n";
        result += "-------------------";
        System.out.println(result);
        workerExecutor.shutdown();

    }

    public static Object testFunc(int size, String msg) {
//        System.out.println("start msg - " + msg);
        int p = 0;
        for (int i = 0; i < size; i++) {
            p = p + i;
        }
        total.incrementAndGet();
//        System.out.println("end msg - " + msg);
        return null;
    }


    private static Object apply(Object[] objects) {
        return testFunc((int) objects[0], (String) objects[1]);
    }
}

