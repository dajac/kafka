/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.util.FutureUtils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// Creating mocks of classes using generics creates unsafe assignment.
@SuppressWarnings("unchecked")
public class CoordinatorExecutorImplTest {
    @Test
    public void testTaskSuccessfulLifecycle() {
        TopicPartition shard = new TopicPartition("__consumer_offsets", 0);

        CoordinatorShard<String> coordinatorShard = mock(CoordinatorShard.class);
        CoordinatorRuntime<CoordinatorShard<String>, String> runtime = mock(CoordinatorRuntime.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<CoordinatorShard<String>, String> executor = new CoordinatorExecutorImpl<>(
            new LogContext(),
            shard,
            runtime,
            executorService
        );

        when(runtime.scheduleWriteOperation(
            any(),
            any(),
            any(),
            any()
        )).thenAnswer(args -> {
            assertTrue(executor.isScheduled("my-task"));
            CoordinatorRuntime.CoordinatorWriteOperation<CoordinatorShard<String>, Void, String> op =
                args.getArgument(3);
            assertEquals(
                new CoordinatorResult<>(Collections.singletonList("record"), null),
                op.generateRecordsAndResult(coordinatorShard)
            );
            return CompletableFuture.completedFuture(null);
        });

        when(executorService.submit(any(Runnable.class))).thenAnswer(args -> {
            assertTrue(executor.isScheduled("my-task"));
            Runnable op = args.getArgument(0);
            op.run();
            return CompletableFuture.completedFuture(null);
        });

        AtomicBoolean taskCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskRunnable<String> taskRunnable = () -> {
            taskCalled.set(true);
            return "Hello!";
        };

        AtomicBoolean operationCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskOperation<String, String> taskOperation = (result, exception) -> {
            operationCalled.set(true);
            assertEquals("Hello!", result);
            assertNull(exception);
            return new CoordinatorResult<>(Collections.singletonList("record"), null);
        };

        executor.schedule(
            "my-task",
            taskRunnable,
            taskOperation
        );

        assertTrue(taskCalled.get());
        assertTrue(operationCalled.get());
    }

    @Test
    public void testTaskFailedLifecycle() {
        TopicPartition shard = new TopicPartition("__consumer_offsets", 0);

        CoordinatorShard<String> coordinatorShard = mock(CoordinatorShard.class);
        CoordinatorRuntime<CoordinatorShard<String>, String> runtime = mock(CoordinatorRuntime.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<CoordinatorShard<String>, String> executor = new CoordinatorExecutorImpl<>(
            new LogContext(),
            shard,
            runtime,
            executorService
        );

        when(runtime.scheduleWriteOperation(
            any(),
            any(),
            any(),
            any()
        )).thenAnswer(args -> {
            CoordinatorRuntime.CoordinatorWriteOperation<CoordinatorShard<String>, Void, String> op =
                args.getArgument(3);
            assertEquals(
                new CoordinatorResult<>(Collections.emptyList(), null),
                op.generateRecordsAndResult(coordinatorShard)
            );
            return CompletableFuture.completedFuture(null);
        });

        when(executorService.submit(any(Runnable.class))).thenAnswer(args -> {
            Runnable op = args.getArgument(0);
            op.run();
            return CompletableFuture.completedFuture(null);
        });

        AtomicBoolean taskCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskRunnable<String> taskRunnable = () -> {
            taskCalled.set(true);
            throw new Exception("Oh no!");
        };

        AtomicBoolean operationCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskOperation<String, String> taskOperation = (result, exception) -> {
            operationCalled.set(true);
            assertNull(result);
            assertNotNull(exception);
            assertEquals("Oh no!", exception.getMessage());
            return new CoordinatorResult<>(Collections.emptyList(), null);
        };

        executor.schedule(
            "my-task",
            taskRunnable,
            taskOperation
        );

        assertTrue(taskCalled.get());
        assertTrue(operationCalled.get());
    }

    @Test
    public void testTaskCancelledBeforeBeingExecuted() {
        TopicPartition shard = new TopicPartition("__consumer_offsets", 0);

        CoordinatorRuntime<CoordinatorShard<String>, String> runtime = mock(CoordinatorRuntime.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<CoordinatorShard<String>, String> executor = new CoordinatorExecutorImpl<>(
            new LogContext(),
            shard,
            runtime,
            executorService
        );

        when(executorService.submit(any(Runnable.class))).thenAnswer(args -> {
            // Cancel the task before running it.
            executor.cancel("my-task");

            // Running the task.
            Runnable op = args.getArgument(0);
            op.run();
            return CompletableFuture.completedFuture(null);
        });

        AtomicBoolean taskCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskRunnable<String> taskRunnable = () -> {
            taskCalled.set(true);
            return null;
        };

        AtomicBoolean operationCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskOperation<String, String> taskOperation = (result, exception) -> {
            operationCalled.set(true);
            return null;
        };

        executor.schedule(
            "my-task",
            taskRunnable,
            taskOperation
        );

        assertFalse(taskCalled.get());
        assertFalse(operationCalled.get());
    }

    @Test
    public void testTaskCancelledAfterBeingExecutedButBeforeWriteOperationIsExecuted() {
        TopicPartition shard = new TopicPartition("__consumer_offsets", 0);

        CoordinatorShard<String> coordinatorShard = mock(CoordinatorShard.class);
        CoordinatorRuntime<CoordinatorShard<String>, String> runtime = mock(CoordinatorRuntime.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<CoordinatorShard<String>, String> executor = new CoordinatorExecutorImpl<>(
            new LogContext(),
            shard,
            runtime,
            executorService
        );

        when(runtime.scheduleWriteOperation(
            any(),
            any(),
            any(),
            any()
        )).thenAnswer(args -> {
            // Cancel the task before running the write operation.
            executor.cancel("my-task");

            CoordinatorRuntime.CoordinatorWriteOperation<CoordinatorShard<String>, Void, String> op =
                args.getArgument(3);
            assertEquals(
                new CoordinatorResult<>(Collections.emptyList(), null),
                op.generateRecordsAndResult(coordinatorShard)
            );
            return CompletableFuture.completedFuture(null);
        });

        when(executorService.submit(any(Runnable.class))).thenAnswer(args -> {
            Runnable op = args.getArgument(0);
            op.run();
            return CompletableFuture.completedFuture(null);
        });

        AtomicBoolean taskCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskRunnable<String> taskRunnable = () -> {
            taskCalled.set(true);
            return "Hello!";
        };

        AtomicBoolean operationCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskOperation<String, String> taskOperation = (result, exception) -> {
            operationCalled.set(true);
            return null;
        };

        executor.schedule(
            "my-task",
            taskRunnable,
            taskOperation
        );

        assertTrue(taskCalled.get());
        assertFalse(operationCalled.get());
    }

    @Test
    public void testTaskSchedulingWriteOperationFailed() {
        TopicPartition shard = new TopicPartition("__consumer_offsets", 0);

        CoordinatorRuntime<CoordinatorShard<String>, String> runtime = mock(CoordinatorRuntime.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<CoordinatorShard<String>, String> executor = new CoordinatorExecutorImpl<>(
            new LogContext(),
            shard,
            runtime,
            executorService
        );

        when(runtime.scheduleWriteOperation(
            any(),
            any(),
            any(),
            any()
        )).thenReturn(FutureUtils.failedFuture(new Throwable("Oh no!")));

        when(executorService.submit(any(Runnable.class))).thenAnswer(args -> {
            Runnable op = args.getArgument(0);
            op.run();
            return CompletableFuture.completedFuture(null);
        });

        AtomicBoolean taskCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskRunnable<String> taskRunnable = () -> {
            taskCalled.set(true);
            return "Hello!";
        };

        AtomicBoolean operationCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskOperation<String, String> taskOperation = (result, exception) -> {
            operationCalled.set(true);
            return new CoordinatorResult<>(Collections.emptyList(), null);
        };

        executor.schedule(
            "my-task",
            taskRunnable,
            taskOperation
        );

        assertTrue(taskCalled.get());
        assertFalse(operationCalled.get());
        assertFalse(executor.isScheduled("my-task"));
    }
}
