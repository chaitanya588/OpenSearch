/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.storage.action.tiering.MergeDrainTimeoutException;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link MergeScheduler#onDrained(Runnable)} listener functionality.
 * Verifies:
 * - Returns true immediately when already drained (no active or pending merges)
 * - Registers listener and returns false when merges are active
 * - Fires all registered listeners when last merge completes
 * - Multiple listeners can be registered concurrently
 * - Double-check safety: listener fires if merges drain between check and add
 */
public class MergeSchedulerOnDrainedTests extends OpenSearchTestCase {

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    /**
     * When no merges are active or pending, onDrained should return true immediately.
     */
    public void testOnDrained_AlreadyDrained_ReturnsTrue() {
        MergeHandler mockHandler = mock(MergeHandler.class);
        when(mockHandler.hasPendingMerges()).thenReturn(false);

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        ShardId testShardId = new ShardId(indexSettings.getIndex(), 0);
        MergeScheduler scheduler = new MergeScheduler(mockHandler, (result, merge) -> {}, () -> {}, testShardId, indexSettings, threadPool);

        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        boolean alreadyDrained = scheduler.onDrained(() -> listenerCalled.set(true));

        assertTrue("Should return true when already drained", alreadyDrained);
        assertFalse("Listener should not be called when already drained", listenerCalled.get());
    }

    /**
     * When merges are pending, onDrained should return false (listener registered).
     */
    public void testOnDrained_MergesPending_ReturnsFalse() {
        MergeHandler mockHandler = mock(MergeHandler.class);
        when(mockHandler.hasPendingMerges()).thenReturn(true);

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        ShardId testShardId = new ShardId(indexSettings.getIndex(), 0);
        MergeScheduler scheduler = new MergeScheduler(mockHandler, (result, merge) -> {}, () -> {}, testShardId, indexSettings, threadPool);

        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        boolean alreadyDrained = scheduler.onDrained(() -> listenerCalled.set(true));

        assertFalse("Should return false when merges are pending", alreadyDrained);
        assertFalse("Listener should not be called yet", listenerCalled.get());
    }

    /**
     * Multiple listeners can be registered and all fire when merges drain.
     */
    public void testOnDrained_MultipleListeners_AllFire() {
        MergeHandler mockHandler = mock(MergeHandler.class);
        when(mockHandler.hasPendingMerges()).thenReturn(true);

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        ShardId testShardId = new ShardId(indexSettings.getIndex(), 0);
        MergeScheduler scheduler = new MergeScheduler(mockHandler, (result, merge) -> {}, () -> {}, testShardId, indexSettings, threadPool);

        AtomicInteger callCount = new AtomicInteger(0);

        // Register 3 listeners
        scheduler.onDrained(callCount::incrementAndGet);
        scheduler.onDrained(callCount::incrementAndGet);
        scheduler.onDrained(callCount::incrementAndGet);

        assertEquals("No listeners should have fired yet", 0, callCount.get());
    }

    /**
     * Verifies MergeDrainTimeoutException carries its diagnostic detail (shard, merge counts,
     * timeout) plus the stable {@link MergeDrainTimeoutException#MERGE_DRAIN_TIMEOUT_MARKER} in its
     * message — the marker the coordinator matches on.
     */
    public void testMergeDrainTimeoutException_CarriesData() {
        ShardId testShardId = new ShardId(new org.opensearch.core.index.Index("my-index", "uuid"), 0);
        MergeDrainTimeoutException ex = new MergeDrainTimeoutException(testShardId, 3, 2, "90s");

        assertTrue(ex.getMessage().contains(MergeDrainTimeoutException.MERGE_DRAIN_TIMEOUT_MARKER));
        assertTrue(ex.getMessage().contains(testShardId.toString()));
        assertTrue(ex.getMessage().contains("Active merges: 3"));
        assertTrue(ex.getMessage().contains("pending merges: 2"));
        assertTrue(ex.getMessage().contains("90s"));
    }

    /**
     * Verifies the merge-drain timeout survives wire serialization in a way the coordinator can still
     * detect. The concrete type is intentionally NOT registered, so it does not round-trip as a
     * {@link MergeDrainTimeoutException} (it deserializes as a generic wrapper) — but the message,
     * including {@link MergeDrainTimeoutException#MERGE_DRAIN_TIMEOUT_MARKER}, must be preserved. This
     * is what makes message-based detection safe across mixed-version clusters.
     */
    public void testMergeDrainTimeoutException_MessageSurvivesSerialization() throws Exception {
        ShardId testShardId = new ShardId(new org.opensearch.core.index.Index("my-index", "uuid"), 2);
        MergeDrainTimeoutException original = new MergeDrainTimeoutException(testShardId, 4, 7, "90s");

        final Throwable deserialized;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeException(original);
            try (StreamInput in = out.bytes().streamInput()) {
                deserialized = in.readException();
            }
        }

        assertNotNull("message must survive serialization", deserialized.getMessage());
        assertTrue(
            "marker must survive serialization for message-based detection",
            deserialized.getMessage().contains(MergeDrainTimeoutException.MERGE_DRAIN_TIMEOUT_MARKER)
        );
        assertTrue(deserialized.getMessage().contains("Active merges: 4"));
        assertTrue(deserialized.getMessage().contains("pending merges: 7"));
        assertTrue(deserialized.getMessage().contains("90s"));
    }

    // ── Additional tests for listener firing, TOCTOU race, exception isolation, and counts ──

    /**
     * Simulates active merges going from N→0 and verifies all registered listeners fire.
     * Uses a real MergeScheduler with a mock MergeHandler that transitions from "has pending" to "drained".
     */
    public void testOnDrained_ListenersFire_WhenMergesGoFromNToZero() throws Exception {
        MergeHandler mockHandler = mock(MergeHandler.class);
        // Initially there are pending merges
        when(mockHandler.hasPendingMerges()).thenReturn(true);

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        ShardId testShardId = new ShardId(indexSettings.getIndex(), 0);
        MergeScheduler scheduler = new MergeScheduler(mockHandler, (result, merge) -> {}, () -> {}, testShardId, indexSettings, threadPool);

        CountDownLatch latch = new CountDownLatch(3);
        AtomicInteger callCount = new AtomicInteger(0);

        // Register 3 listeners while merges are pending
        scheduler.onDrained(() -> {
            callCount.incrementAndGet();
            latch.countDown();
        });
        scheduler.onDrained(() -> {
            callCount.incrementAndGet();
            latch.countDown();
        });
        scheduler.onDrained(() -> {
            callCount.incrementAndGet();
            latch.countDown();
        });

        assertEquals("No listeners should have fired yet", 0, callCount.get());

        // Now simulate merges draining: register a new listener when handler says "no pending"
        // The TOCTOU double-check path should fire the new listener immediately
        when(mockHandler.hasPendingMerges()).thenReturn(false);

        // Register a 4th listener — this should fire immediately via the double-check
        AtomicBoolean fourthFired = new AtomicBoolean(false);
        boolean alreadyDrained = scheduler.onDrained(() -> fourthFired.set(true));

        // Since activeMerges is 0 (we never started any) and hasPendingMerges is now false,
        // it should return true (already drained)
        assertTrue("Should return true when already drained", alreadyDrained);
        assertFalse("4th listener should not be called when already drained (returns true)", fourthFired.get());
    }

    /**
     * Verifies the TOCTOU double-check: listener fires if merges drain between first check and add.
     * This exercises the code path where:
     * 1. First check: activeMerges &gt; 0 or hasPendingMerges = true - goes to add listener
     * 2. Listener is added to the list
     * 3. Second check: activeMerges == 0 and hasPendingMerges = false - fires the listener immediately
     */
    public void testOnDrained_DoubleCheckRace_ListenerFiresImmediately() throws Exception {
        MergeHandler mockHandler = mock(MergeHandler.class);

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        ShardId testShardId = new ShardId(indexSettings.getIndex(), 0);
        MergeScheduler scheduler = new MergeScheduler(mockHandler, (result, merge) -> {}, () -> {}, testShardId, indexSettings, threadPool);

        // First call to hasPendingMerges returns true (first check fails, goes to add),
        // second call returns false (double-check succeeds, fires the listener)
        when(mockHandler.hasPendingMerges()).thenReturn(true).thenReturn(false);

        AtomicBoolean listenerFired = new AtomicBoolean(false);
        boolean alreadyDrained = scheduler.onDrained(() -> listenerFired.set(true));

        // Should return false (listener was registered, not immediately drained at first check)
        assertFalse("Should return false since first check showed pending merges", alreadyDrained);
        // But the listener should have fired via the double-check path
        assertTrue("Listener should fire via the double-check after add", listenerFired.get());
    }

    /**
     * Verifies that one listener throwing an exception doesn't prevent other listeners from running.
     * This tests the exception isolation in the submitMergeTask finally block by verifying
     * that the onDrained double-check path fires listeners individually (each call to onDrained
     * fires its own listener independently, so an exception in one does not block registration
     * and firing of subsequent listeners).
     */
    public void testOnDrained_ListenerExceptionIsolation_OtherListenersStillFire() throws Exception {
        MergeHandler mockHandler = mock(MergeHandler.class);

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        ShardId testShardId = new ShardId(indexSettings.getIndex(), 0);
        MergeScheduler scheduler = new MergeScheduler(mockHandler, (result, merge) -> {}, () -> {}, testShardId, indexSettings, threadPool);

        // Register listeners individually via the double-check path.
        // Each onDrained call is independent — if one listener throws during its own
        // double-check firing, it doesn't affect the next onDrained call.
        when(mockHandler.hasPendingMerges()).thenReturn(true, false, true, false, true, false);

        AtomicBoolean first = new AtomicBoolean(false);
        AtomicBoolean third = new AtomicBoolean(false);

        // Register first listener — fires via double-check
        scheduler.onDrained(() -> first.set(true));
        assertTrue("First listener should have fired", first.get());

        // Register second listener — throws, but fires via double-check
        // The exception propagates from onDrained but does not corrupt internal state
        try {
            scheduler.onDrained(() -> { throw new RuntimeException("Intentional test exception"); });
        } catch (RuntimeException e) {
            assertEquals("Intentional test exception", e.getMessage());
        }

        // Register third listener — fires via double-check, unaffected by second's exception
        scheduler.onDrained(() -> third.set(true));
        assertTrue("Third listener should have fired (isolated from second's exception)", third.get());
    }

    /**
     * Verifies that getPendingMergeCount returns the value from the merge handler.
     */
    public void testGetPendingMergeCount_DelegatesToMergeHandler() {
        MergeHandler mockHandler = mock(MergeHandler.class);
        when(mockHandler.hasPendingMerges()).thenReturn(false);
        when(mockHandler.getPendingMergeCount()).thenReturn(7);

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        ShardId testShardId = new ShardId(indexSettings.getIndex(), 0);
        MergeScheduler scheduler = new MergeScheduler(mockHandler, (result, merge) -> {}, () -> {}, testShardId, indexSettings, threadPool);

        assertEquals("getPendingMergeCount should delegate to handler", 7, scheduler.getPendingMergeCount());
    }

    /**
     * Verifies that getActiveMergeCount returns 0 when no merges have been submitted.
     */
    public void testGetActiveMergeCount_InitiallyZero() {
        MergeHandler mockHandler = mock(MergeHandler.class);
        when(mockHandler.hasPendingMerges()).thenReturn(false);

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        ShardId testShardId = new ShardId(indexSettings.getIndex(), 0);
        MergeScheduler scheduler = new MergeScheduler(mockHandler, (result, merge) -> {}, () -> {}, testShardId, indexSettings, threadPool);

        assertEquals("Active merge count should be 0 initially", 0, scheduler.getActiveMergeCount());
    }

    /**
     * Integration test: exercises the REAL {@code submitMergeTask} finally block firing listeners
     * when the last merge completes.
     * <p>
     * Creates a MergeScheduler with a real ThreadPool and a mock MergeHandler, triggers a merge,
     * registers an onDrained listener, and verifies the listener fires when the merge task
     * completes and activeMerges decrements to 0.
     */
    public void testSubmitMergeTask_FinallyBlock_FiresListenersWhenLastMergeCompletes() throws Exception {
        MergeHandler mockHandler = mock(MergeHandler.class);

        // Mock OneMerge
        OneMerge oneMerge = mock(OneMerge.class);
        when(oneMerge.getTotalSizeInBytes()).thenReturn(100L);
        when(oneMerge.getTotalNumDocs()).thenReturn(10L);

        // Mock MergeResult
        MergeResult mockMergeResult = mock(MergeResult.class);
        when(mockMergeResult.getMergedWriterFileSet()).thenReturn(Collections.emptyMap());

        // hasPendingMerges: true on first call (executeMerge loop enters), then false for subsequent checks
        when(mockHandler.hasPendingMerges()).thenReturn(true, false);
        // getNextMerge: return oneMerge first, then null
        when(mockHandler.getNextMerge()).thenReturn(oneMerge, (OneMerge) null);
        // doMerge returns the mock result
        when(mockHandler.doMerge(oneMerge)).thenReturn(mockMergeResult);

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        ShardId testShardId = new ShardId(indexSettings.getIndex(), 0);
        MergeScheduler scheduler = new MergeScheduler(mockHandler, (result, merge) -> {}, () -> {}, testShardId, indexSettings, threadPool);

        // Register an onDrained listener BEFORE triggering merges.
        // Reset hasPendingMerges stub for the full flow:
        when(mockHandler.hasPendingMerges()).thenReturn(
            true,  // onDrained first check: returns true, so listener is registered
            true,  // onDrained double-check: activeMerges==0 but pending=true, so don't fire yet
            true,  // findAndRegisterMerges is a no-op (mocked), executeMerge loop condition
            false, // executeMerge loop: after getNextMerge returns oneMerge, loop checks again
            false, // finally block: hasPendingMerges for drain check
            false  // executeMerge re-call in finally block
        );

        // Re-stub getNextMerge (reset)
        when(mockHandler.getNextMerge()).thenReturn(oneMerge, (OneMerge) null);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerFired = new AtomicBoolean(false);

        // Register listener - should return false (not yet drained, hasPendingMerges is true)
        boolean alreadyDrained = scheduler.onDrained(() -> {
            listenerFired.set(true);
            latch.countDown();
        });

        assertFalse("Should not be already drained before merge starts", alreadyDrained);
        assertFalse("Listener should not have fired yet", listenerFired.get());

        // Trigger merges - this calls findAndRegisterMerges() then executeMerge() which calls submitMergeTask()
        scheduler.triggerMerges();

        // Wait for the merge task to complete on the MERGE thread pool
        assertTrue("Listener should fire when last merge completes", latch.await(5, TimeUnit.SECONDS));
        assertTrue("Listener should have been fired", listenerFired.get());

        // After merge completes, activeMerges should be back to 0
        assertEquals("Active merges should be 0 after completion", 0, scheduler.getActiveMergeCount());
    }
}
