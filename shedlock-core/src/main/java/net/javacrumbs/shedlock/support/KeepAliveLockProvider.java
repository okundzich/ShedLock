package net.javacrumbs.shedlock.support;

import net.javacrumbs.shedlock.core.AbstractSimpleLock;
import net.javacrumbs.shedlock.core.ExtensibleLockProvider;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import net.javacrumbs.shedlock.support.annotation.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * LockProvider that keeps the lock `alive`. In the middle of lockAtMostFor period tries to extend the lock for
 * lockAtMostFor period. For example, if the lockAtMostFor is 10 minutes the lock is extended every 5 minutes for 10 minutes
 * until the lock is released. If the process dies, the lock is automatically released after lockAtMostFor period as usual.
 *
 * Wraps ExtensibleLockProvider that implements the actual locking.
 */
public class KeepAliveLockProvider implements LockProvider {
    private final ExtensibleLockProvider wrapped;
    private final ScheduledExecutorService executorService;

    private static final Logger logger = LoggerFactory.getLogger(KeepAliveLockProvider.class);

    public KeepAliveLockProvider(ExtensibleLockProvider wrapped, ScheduledExecutorService executorService) {
        this.wrapped = wrapped;
        this.executorService = executorService;
    }

    @Override
    @NonNull
    public Optional<SimpleLock> lock(@NonNull LockConfiguration lockConfiguration) {
        Optional<SimpleLock> lock = wrapped.lock(lockConfiguration);
        return lock.map(simpleLock -> new KeepAliveLock(lockConfiguration, simpleLock, executorService));
    }

    private static class KeepAliveLock extends AbstractSimpleLock {
        private final Duration lockExtensionPeriod;
        private SimpleLock lock;
        private Duration remainingLockAtLeastFor;
        private final ScheduledFuture<?> future;
        private boolean unlocked = false;

        private KeepAliveLock(LockConfiguration lockConfiguration, SimpleLock lock, ScheduledExecutorService executorService) {
            super(lockConfiguration);
            this.lock = lock;
            this.lockExtensionPeriod = lockConfiguration.getLockAtMostFor().dividedBy(2);
            this.remainingLockAtLeastFor = lockConfiguration.getLockAtLeastFor();

            long extensionPeriodMs = lockExtensionPeriod.toMillis();
            this.future = executorService.scheduleAtFixedRate(
                this::extendForNextPeriod,
                extensionPeriodMs,
                extensionPeriodMs,
                MILLISECONDS
            );
        }

        private void extendForNextPeriod() {
            // We can have a race-condition when we extend the lock but the `lock` field is accessed before we update it.
            synchronized (this) {
                if (unlocked) {
                    return;
                }
                remainingLockAtLeastFor = remainingLockAtLeastFor.minus(lockExtensionPeriod);
                if (remainingLockAtLeastFor.isNegative()) {
                    remainingLockAtLeastFor = Duration.ZERO;
                }
                Optional<SimpleLock> extendedLock = lock.extend(lockConfiguration.getLockAtMostFor(), remainingLockAtLeastFor);
                if (extendedLock.isPresent()) {
                    lock = extendedLock.get();
                    logger.trace("Lock {} extended for {}", lockConfiguration.getName(), lockConfiguration.getLockAtMostFor());
                } else {
                    logger.warn("Can't extend lock {}", lockConfiguration.getName());
                    stop();
                }
            }
        }

        private void stop() {
            future.cancel(false);
        }

        @Override
        protected void doUnlock() {
            stop();
            synchronized (this) {
                lock.unlock();
                unlocked = true;
            }
        }

        @Override
        protected Optional<SimpleLock> doExtend(LockConfiguration newConfiguration) {
            throw new UnsupportedOperationException("Manual extension of KeepAliveLock is not supported (yet)");
        }
    }
}
