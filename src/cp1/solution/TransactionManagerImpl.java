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

    private final Semaphore syncSem;
    private final ConcurrentMap<ResourceId, Resource> ridToResrc;
    private final ConcurrentMap<ResourceId, Thread> ridToOwner;
    private final ConcurrentMap<ResourceId, Semaphore> ridToSem;
    private final ConcurrentMap<ResourceId, Semaphore> ridToTranSem;
    private final ConcurrentMap<Thread, Set<ResourceId>> threadToOpResrc;
    private final ConcurrentMap<Thread, Deque<TransactionOperation>> threadToOpResrcSeq;
    private final ConcurrentMap<Thread, ResourceId> threadToAwaitedResrc;
    private final ConcurrentMap<Thread, Long> threadToTranStartTime;
    private final Set<Thread> abortedThreads;
    private final LocalTimeProvider timeProvider;

    public TransactionManagerImpl(Collection<Resource> resources,
                                  LocalTimeProvider timeProvider) {
        syncSem = new Semaphore(1, true);
        ridToResrc = new ConcurrentSkipListMap<>();
        resources.forEach(resource -> ridToResrc.put(resource.getId(), resource));
        ridToOwner = new ConcurrentSkipListMap<>();
        ridToSem = new ConcurrentSkipListMap<>();
        resources.forEach(resource -> ridToSem.put(resource.getId(), new Semaphore(1, true)));
        ridToTranSem = new ConcurrentSkipListMap<>();
        resources.forEach(resource -> ridToTranSem.put(resource.getId(), new Semaphore(1, true)));
        threadToOpResrc = new ConcurrentHashMap<>();
        threadToOpResrcSeq = new ConcurrentHashMap<>();
        threadToAwaitedResrc = new ConcurrentHashMap<>();
        threadToTranStartTime = new ConcurrentHashMap<>();
        abortedThreads = new ConcurrentSkipListSet<>(Comparator.comparingLong(Thread::getId));
        this.timeProvider = timeProvider;
    }

    @Override
    public void startTransaction() throws AnotherTransactionActiveException {
        if (isTransactionActive()) {
            throw new AnotherTransactionActiveException();
        } else {
            Thread currentThread = Thread.currentThread();

            abortedThreads.remove(currentThread);
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

            Semaphore resrcOwnerSem = ridToSem.get(rid);
            resrcOwnerSem.acquire();

            Semaphore resrcTranSem = ridToTranSem.get(rid);

            if (currentThread.equals(ridToOwner.getOrDefault(rid, currentThread))) {
                if (!ridToOwner.containsKey(rid)) {
                    resrcTranSem.acquire();
                }
            } else {
                Thread resrcOwner = ridToOwner.get(rid);
                resrcOwnerSem.release();

                syncSem.acquire();
                threadToAwaitedResrc.put(currentThread, rid);
                detectDeadlock(currentThread, resrcOwner);
                syncSem.release();

                resrcTranSem.acquire();
            }

            useResource(rid, operation, currentThread, resrcOwnerSem);
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
                next = ridToOwner.get(awaitedResrc);
            }
        }

        return deadlock;
    }

    private void useResource(ResourceId rid, ResourceOperation operation, Thread currentThread,
                             Semaphore resrcOwnerSem)
            throws ResourceOperationException, InterruptedException {
        ridToOwner.put(rid, currentThread);
        resrcOwnerSem.release();

        if (threadToAwaitedResrc.containsKey(currentThread)) {
            syncSem.acquire();
            threadToAwaitedResrc.remove(currentThread);
            syncSem.release();
        }

        threadToOpResrc.get(currentThread)
                       .add(rid);
        ridToResrc.get(rid)
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

        syncSem.acquireUninterruptibly();
        threadToAwaitedResrc.remove(currentThread);
        syncSem.release();

        undoTransactionOperations(opResrcSeq);
        freeTransactionResources(opResrcSeq);
    }

    @Override
    public boolean isTransactionActive() {
        return threadToOpResrcSeq.containsKey(Thread.currentThread());
    }

    @Override
    public boolean isTransactionAborted() {
        return abortedThreads.contains(Thread.currentThread());
    }

    private boolean isKnownResourceId(ResourceId rid) {
        return ridToResrc.containsKey(rid);
    }

    private void freeResource(TransactionOperation ridOp) {
        Semaphore resrcOwnerSem = ridToSem.get(ridOp.rid);
        resrcOwnerSem.acquireUninterruptibly();

        ridToOwner.remove(ridOp.rid);

        Semaphore resrcTranSem = ridToTranSem.get(ridOp.rid);
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
                freeResource(ridOp);
                opResrc.remove(ridOp.rid);
            }
        }

        abortedThreads.remove(currentThread);
        threadToOpResrc.remove(currentThread);
        threadToOpResrcSeq.remove(currentThread);
        threadToTranStartTime.remove(currentThread);
    }

    private void undoTransactionOperations(Deque<TransactionOperation> opResrcSeq) {
        List<TransactionOperation> tmpOpResrcSeq = new ArrayList<>(opResrcSeq);
        Collections.reverse(tmpOpResrcSeq);

        for (TransactionOperation ridOp : tmpOpResrcSeq) {
            Resource resource = ridToResrc.get(ridOp.rid);
            resource.unapply(ridOp.operation);
        }
    }

}
