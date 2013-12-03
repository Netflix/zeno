/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.zeno.util;

import java.util.concurrent.ExecutionException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;

/**
 * A {@link SimultaneousExecutor} which throws an Exception on awaitUniterruptibly(), if
 * any of the tasks did not finish.
 *
 * @author dsu
 *
 */
public class EnsureSuccessSimultaneousExecutor extends SimultaneousExecutor {
    private final List<Future<?>> futures = new ArrayList<Future<?>>();
    public EnsureSuccessSimultaneousExecutor() {
    }

    public EnsureSuccessSimultaneousExecutor(double threadsPerCpu) {
        super(threadsPerCpu);
    }

    public EnsureSuccessSimultaneousExecutor(double threadsPerCpu, String threadName) {
        super(threadsPerCpu, threadName);
    }

    public EnsureSuccessSimultaneousExecutor(int numThreads) {
        super(numThreads);
    }

    public EnsureSuccessSimultaneousExecutor(int numThreads, String threadName) {
        super(numThreads, threadName);
    }

    @Override
    protected final <T> RunnableFuture<T> newTaskFor(final Runnable runnable, final T value) {
        final RunnableFuture<T> task = super.newTaskFor(runnable, value);
        futures.add(task);
        return task;
    }

    @Override
    protected final <T> RunnableFuture<T> newTaskFor(final Callable<T> callable) {
        final RunnableFuture<T> task = super.newTaskFor(callable);
        futures.add(task);
        return task;
    }

    /**
     * Await successful completion of all tasks. throw exception for the first task that fails.
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws Exception
     */
    public void awaitSuccessfulCompletion() throws InterruptedException, ExecutionException {
        awaitUninterruptibly();
        for(final Future<?> f:futures) {
            f.get();
        }
    }
}
