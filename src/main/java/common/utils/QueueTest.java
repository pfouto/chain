package common.utils;

import babel.events.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class QueueTest {

    private static final InternalEvent.EventType[] DEFAULT_PRIORITY = {
            InternalEvent.EventType.TIMER_EVENT,
            InternalEvent.EventType.NOTIFICATION_EVENT,
            InternalEvent.EventType.MESSAGE_IN_EVENT,
            InternalEvent.EventType.MESSAGE_FAILED_EVENT,
            InternalEvent.EventType.MESSAGE_SENT_EVENT,
            InternalEvent.EventType.IPC_EVENT,
            InternalEvent.EventType.CUSTOM_CHANNEL_EVENT,
    };


    public static void main(String[] args) throws InterruptedException {

        int N_ELEMS = 20000000;

        EventPriorityQueue akosQueue = new EventPriorityQueue();
        BetterEventPriorityQueue bAkosQueue = new BetterEventPriorityQueue();
        BlockingQueue<InternalEvent> javaBlockingQueue = new LinkedBlockingQueue<>();
        PriorityBlockingQueue<InternalEvent> javaPBlockingQueue = new PriorityBlockingQueue<>(N_ELEMS, (o1, o2) -> {
            int po1 = -1;
            int po2 = -1;
            for (int i = 0; i < DEFAULT_PRIORITY.length; i++) {
                if (o1.getType() == DEFAULT_PRIORITY[i]) po1 = i;
                if (o2.getType() == DEFAULT_PRIORITY[i]) po2 = i;
            }
            return po1 - po2;
        });

        System.out.println(" ----- 1 Prod");
        System.out.println(" JAVA B QUEUE: " + test(new Random(2000), javaBlockingQueue, N_ELEMS,1));
        System.out.println(" AKOS QUEUE: " + test(new Random(2000), akosQueue, N_ELEMS,1));
        System.out.println(" BAKOS QUEUE: " + test(new Random(2000), bAkosQueue, N_ELEMS,1));
        System.out.println(" JAVA PB QUEUE: " + test(new Random(2000), javaPBlockingQueue, N_ELEMS,1));
        System.out.println(" ----- 5 Prod");
        System.out.println(" JAVA B QUEUE: " + test(new Random(3000), javaBlockingQueue, N_ELEMS,5));
        System.out.println(" AKOS QUEUE: " + test(new Random(3000), akosQueue, N_ELEMS,5));
        System.out.println(" BAKOS QUEUE: " + test(new Random(3000), bAkosQueue, N_ELEMS,5));
        System.out.println(" JAVA PB QUEUE: " + test(new Random(3000), javaPBlockingQueue, N_ELEMS,5));
        System.out.println(" ----- 25 Prod");
        System.out.println(" JAVA B QUEUE: " + test(new Random(4000), javaBlockingQueue, N_ELEMS,25));
        System.out.println(" AKOS QUEUE: " + test(new Random(4000), akosQueue, N_ELEMS,25));
        System.out.println(" BAKOS QUEUE: " + test(new Random(4000), bAkosQueue, N_ELEMS,25));
        System.out.println(" JAVA PB QUEUE: " + test(new Random(4000), javaPBlockingQueue, N_ELEMS,25));
        System.out.println(" ----- 100 Prod");
        System.out.println(" JAVA B QUEUE: " + test(new Random(5000), javaBlockingQueue, N_ELEMS,100));
        System.out.println(" AKOS QUEUE: " + test(new Random(5000), akosQueue, N_ELEMS,100));
        System.out.println(" BAKOS QUEUE: " + test(new Random(5000), bAkosQueue, N_ELEMS,100));
        System.out.println(" JAVA PB QUEUE: " + test(new Random(5000), javaPBlockingQueue, N_ELEMS,100));
    }

    private static long test(Random r, BlockingQueue<InternalEvent> q, int nElements, int nProducers) throws InterruptedException {
        List<Thread> producers = new LinkedList<>();
        for(int i = 0;i<nProducers;i++)
            producers.add(new Thread(() -> fillQueue(new Random(r.nextInt()), q, nElements/nProducers)));

        Thread consumer = new Thread(() -> {
            try {
                for (int i = 0; i < nElements; i++)
                    q.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        /*new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.gc();
            }
        }).start();*/
        long time = System.currentTimeMillis();
        producers.forEach(Thread::start);
        consumer.start();

        for (Thread producer : producers) {
            producer.join();
        }
        //System.out.println("Producers ended");

        consumer.join();
        return System.currentTimeMillis() - time;
    }

    private static void fillQueue(Random r, BlockingQueue<InternalEvent> q, int nElements) {
        for (int i = 0; i < nElements; i++) {
            int type = r.nextInt(DEFAULT_PRIORITY.length);
            InternalEvent.EventType eventType = DEFAULT_PRIORITY[type];
            switch (eventType) {
                case IPC_EVENT:
                    q.add(new IPCEvent(null, (short) 0, (short) 0));
                    break;
                case TIMER_EVENT:
                    q.add(new TimerEvent(null, 10, null, 10, false, 10));
                    break;
                case MESSAGE_IN_EVENT:
                    q.add(new MessageInEvent(null, null, (short) 0));
                    break;
                case MESSAGE_SENT_EVENT:
                    q.add(new MessageSentEvent(null, null, (short) 0));
                    break;
                case NOTIFICATION_EVENT:
                    q.add(new NotificationEvent(null, (short) 0));
                    break;
                case CUSTOM_CHANNEL_EVENT:
                    q.add(new CustomChannelEvent(null, (short) 0));
                    break;
                case MESSAGE_FAILED_EVENT:
                    q.add(new MessageFailedEvent(null, null, null, (short) 0));
                    break;
                default:
                    throw new AssertionError("ups");
            }
        }
    }
}
