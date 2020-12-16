package cp1.solution;

import cp1.base.*;

import java.util.*;
import java.util.concurrent.*;

public class TransactionManagerImpl implements TransactionManager {

    private static class TransactionOperation {

        private final ResourceId rid;
        private final ResourceOperation operation;

        private TransactionOperation(ResourceId rid, ResourceOperation operation) {
            this.rid = rid;
            this.operation = operation;
        }

    }

    private static final Comparator<Thread> THREADS_COMPARATOR;

    static {
        THREADS_COMPARATOR = (t1, t2) -> Long.compare(t1.getId(), t2.getId());
    }

    private final Semaphore syncSem;
    private final ConcurrentMap<ResourceId, Resource> resourceIdToResource;
    private final ConcurrentMap<ResourceId, Thread> resourceIdToThread;
    private final ConcurrentMap<ResourceId, Semaphore> resourceIdToSem;
    private final ConcurrentMap<ResourceId, Semaphore> resourceIdToTranSem;
    private final ConcurrentMap<Thread, Set<ResourceId>> threadToOpResrc;
    private final ConcurrentMap<Thread, Deque<TransactionOperation>> threadToOpResrcSeq;
    private final ConcurrentMap<Thread, ResourceId> threadToAwaitedResrc;
    private final ConcurrentMap<Thread, Long> threadToTranStartTime;
    private final Set<Thread> abortedThreads;
    private final LocalTimeProvider timeProvider;

    public TransactionManagerImpl(Collection<Resource> resources,
                                  LocalTimeProvider timeProvider) {
        syncSem = new Semaphore(1, true);

        resourceIdToResource = new ConcurrentSkipListMap<>();
        resources.forEach(resource -> resourceIdToResource.put(resource.getId(), resource));

        resourceIdToThread = new ConcurrentSkipListMap<>();

        resourceIdToSem = new ConcurrentSkipListMap<>();
        resources.forEach(
                resource -> resourceIdToSem.put(resource.getId(), new Semaphore(1, true)));

        resourceIdToTranSem = new ConcurrentSkipListMap<>();
        resources.forEach(
                resource -> resourceIdToTranSem.put(resource.getId(), new Semaphore(1, true)));

        threadToOpResrc = new ConcurrentHashMap<>();

        threadToOpResrcSeq = new ConcurrentHashMap<>();

        threadToAwaitedResrc = new ConcurrentHashMap<>();

        threadToTranStartTime = new ConcurrentHashMap<>();

        abortedThreads = new ConcurrentSkipListSet<>(THREADS_COMPARATOR);

        this.timeProvider = timeProvider;
    }

    @Override
    public void startTransaction() throws AnotherTransactionActiveException {
        if (isTransactionActive()) {
            throw new AnotherTransactionActiveException();
        } else {
            Thread currentThread = Thread.currentThread();
            threadToOpResrc.put(currentThread, new ConcurrentSkipListSet<>());
            threadToOpResrcSeq.put(currentThread, new ConcurrentLinkedDeque<>());
            threadToTranStartTime.put(currentThread, timeProvider.getTime());
        }
    }

    @Override
    public void operateOnResourceInCurrentTransaction(ResourceId rid, ResourceOperation operation)
            throws NoActiveTransactionException, UnknownResourceIdException,
                   ActiveTransactionAborted, ResourceOperationException, InterruptedException {
        if (!isTransactionActive()) {
            throw new NoActiveTransactionException();
        } else if (!isKnownResourceId(rid)) {
            throw new UnknownResourceIdException(rid);
        } else if (isTransactionAborted()) {
            throw new ActiveTransactionAborted();
        } else {
            Thread currentThread = Thread.currentThread();

            Semaphore resrcOwnerSem = resourceIdToSem.get(rid);
            resrcOwnerSem.acquire();

            Semaphore resrcTranSem = resourceIdToTranSem.get(rid);

            if (currentThread.equals(resourceIdToThread.getOrDefault(rid, currentThread))) {
                if (!resourceIdToThread.containsKey(rid)) {
                    resrcTranSem.acquire();
                }
            } else {
                Thread resrcOwner = resourceIdToThread.get(rid);
                resrcOwnerSem.release();

                syncSem.acquire();
                threadToAwaitedResrc.put(currentThread, rid);
                detectDeadlock(currentThread, resrcOwner);
                syncSem.release();

                resrcTranSem.acquire();
            }

            useResource(rid, operation, currentThread, resrcOwnerSem);
            resrcTranSem.release();
        }
    }

    private Thread getThreadToAbort(List<Thread> owners) {
        return owners.stream()
                     .max((t1, t2) -> {
                         long t1TranStartTime = threadToTranStartTime.get(t1);
                         long t2TranStartTime = threadToTranStartTime.get(t2);

                         if (t1TranStartTime == t2TranStartTime) {
                             return Long.compare(t1.getId(), t2.getId());
                         } else {
                             return Long.compare(t1TranStartTime, t2TranStartTime);
                         }
                     })
                     .get();
    }

    private void detectDeadlock(Thread currentThread, Thread next) {
        List<Thread> owners = new ArrayList<>();
        owners.add(currentThread);

        if (isDeadlock(currentThread, next, owners)) {
            Thread threadToAbort = getThreadToAbort(owners);
            abortedThreads.add(threadToAbort);
            threadToAbort.interrupt();
        }
    }

    private boolean isDeadlock(Thread currentThread, Thread next, List<Thread> owners) {
        Set<ResourceId> currentThreadResrc = threadToOpResrc.get(currentThread);

        boolean deadlock = false;
        while (true) {
            if (!threadToAwaitedResrc.containsKey(next) || abortedThreads.contains(next)) {
                break;
            }
            owners.add(next);
            ResourceId awaitedResrc = threadToAwaitedResrc.get(next);
            if (currentThreadResrc.contains(awaitedResrc)) {
                deadlock = true;
                break;
            } else {
                next = resourceIdToThread.get(awaitedResrc);
            }
        }

        return deadlock;
    }

    private void useResource(ResourceId rid, ResourceOperation operation, Thread currentThread,
                             Semaphore resrcOwnerSem)
            throws ResourceOperationException, InterruptedException {
        resourceIdToThread.put(rid, currentThread);

        syncSem.acquire();
        threadToAwaitedResrc.remove(currentThread);
        syncSem.release();
        resrcOwnerSem.release();

        threadToOpResrc.get(currentThread)
                       .add(rid);
        resourceIdToResource.get(rid)
                            .apply(operation);
        threadToOpResrcSeq.get(currentThread)
                          .addLast(new TransactionOperation(rid, operation));
    }

    @Override
    public void commitCurrentTransaction()
            throws NoActiveTransactionException, ActiveTransactionAborted {
        if (!isTransactionActive()) {
            throw new NoActiveTransactionException();
        } else if (isTransactionAborted()) {
            throw new ActiveTransactionAborted();
        } else {
            freeTransactionResources(threadToOpResrcSeq.getOrDefault(Thread.currentThread(),
                                                                     new ConcurrentLinkedDeque<>()));
        }
    }

    @Override
    public void rollbackCurrentTransaction() {
        Thread currentThread = Thread.currentThread();
        Deque<TransactionOperation> opResrcSeq =
                threadToOpResrcSeq.getOrDefault(currentThread, new ConcurrentLinkedDeque<>());

        try {
            syncSem.acquire();
            threadToAwaitedResrc.remove(currentThread);
            syncSem.release();
        } catch (InterruptedException e) {
            e.printStackTrace();
            currentThread.interrupt();
            return;
        }

        undoTransactionOperations(opResrcSeq);
        freeTransactionResources(opResrcSeq);
    }

    @Override
    public boolean isTransactionActive() {
        return !isTransactionAborted() && threadToOpResrcSeq.containsKey(Thread.currentThread());
    }

    @Override
    public boolean isTransactionAborted() {
        return abortedThreads.contains(Thread.currentThread());
    }

    private boolean isKnownResourceId(ResourceId rid) {
        return resourceIdToResource.containsKey(rid);
    }

    private void freeResource(Thread currentThread, TransactionOperation ridOp) {
        Semaphore resrcOwnerSem = resourceIdToSem.get(ridOp.rid);
        try {
            resrcOwnerSem.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            currentThread.interrupt();
            return;
        }

        resourceIdToThread.remove(ridOp.rid);

        Semaphore resrcTranSem = resourceIdToTranSem.get(ridOp.rid);
        if (!resrcTranSem.hasQueuedThreads()) {
            resrcOwnerSem.release();
        }
        resrcTranSem.release();
    }

    private void freeTransactionResources(Deque<TransactionOperation> opResrcSeq) {
        Thread currentThread = Thread.currentThread();
        Set<ResourceId> opResrc = threadToOpResrc.get(currentThread);

        while (opResrcSeq.size() > 0) {
            TransactionOperation ridOp = opResrcSeq.removeLast();
            if (opResrc.contains(ridOp.rid)) {
                freeResource(currentThread, ridOp);
                opResrc.remove(ridOp.rid);
            }
        }

        threadToOpResrc.remove(currentThread);
        threadToOpResrcSeq.remove(currentThread);
        threadToTranStartTime.remove(currentThread);
    }

    private void undoTransactionOperations(Deque<TransactionOperation> opResrcSeq) {
        List<TransactionOperation> tmpOpResrcSeq = new ArrayList<>(opResrcSeq);
        Collections.reverse(tmpOpResrcSeq);

        for (TransactionOperation ridOp : tmpOpResrcSeq) {
            Resource resource = resourceIdToResource.get(ridOp.rid);
            resource.unapply(ridOp.operation);
        }
    }

}
