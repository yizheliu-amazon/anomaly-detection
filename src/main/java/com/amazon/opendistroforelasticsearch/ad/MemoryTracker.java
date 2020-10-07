/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE;

import java.util.EnumMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.monitor.jvm.JvmService;

import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.randomcutforest.RandomCutForest;

/**
 * Class to track AD memory usage.
 *
 */
public class MemoryTracker {
    private static final Logger LOG = LogManager.getLogger(MemoryTracker.class);

    public enum Origin {
        SINGLE_ENTITY_DETECTOR,
        MULTI_ENTITY_DETECTOR
    }

    // A tree of N samples has 2N nodes, with one bounding box for each node.
    private static final int BOUNDING_BOXES = 2;
    // A bounding box has one vector for min values and one for max.
    private static final int VECTORS_IN_BOUNDING_BOX = 2;

    // memory tracker for total consumption of bytes
    private long totalMemoryBytes;
    private final Map<Origin, Long> totalMemoryBytesByOrigin;
    // reserved for models. Cannot be deleted at will.
    private long reservedMemoryBytes;
    private final Map<Origin, Long> reservedMemoryBytesByOrigin;
    private long heapSize;
    private long heapLimitBytes;
    private long desiredModelSize;
    // constant used to compute an rcf model size
    private long rcfSizeConstant;

    /**
     * Constructor
     *
     * @param jvmService Service providing jvm info
     * @param modelMaxSizePercentage Percentage of heap for the max size of a model
     * @param modelDesiredSizePercentage percentage of heap for the desired size of a model
     * @param clusterService Cluster service object
     * @param sampleSize The sample size used by stream samplers in a RCF forest
     */
    public MemoryTracker(
        JvmService jvmService,
        double modelMaxSizePercentage,
        double modelDesiredSizePercentage,
        ClusterService clusterService,
        int sampleSize
    ) {
        this.totalMemoryBytes = 0;
        this.totalMemoryBytesByOrigin = new EnumMap<Origin, Long>(Origin.class);
        this.reservedMemoryBytes = 0;
        this.reservedMemoryBytesByOrigin = new EnumMap<Origin, Long>(Origin.class);
        this.heapSize = jvmService.info().getMem().getHeapMax().getBytes();
        this.heapLimitBytes = (long) (heapSize * modelMaxSizePercentage);
        this.desiredModelSize = (long) (heapSize * modelDesiredSizePercentage);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MODEL_MAX_SIZE_PERCENTAGE, it -> this.heapLimitBytes = (long) (heapSize * it));
        this.rcfSizeConstant = sampleSize * BOUNDING_BOXES * VECTORS_IN_BOUNDING_BOX * (Long.SIZE / Byte.SIZE);
    }

    public synchronized boolean isHostingAllowed(String detectorId, RandomCutForest rcf) {
        return canAllocateReserved(detectorId, estimateModelSize(rcf));
    }

    /**
     * @param detectorId Detector Id, used in error message
     * @param requiredBytes required bytes in memory
     * @return whether there is memory required for AD
     */
    public synchronized boolean canAllocateReserved(String detectorId, long requiredBytes) {
        if (reservedMemoryBytes + requiredBytes <= heapLimitBytes) {
            return true;
        } else {
            throw new LimitExceededException(
                detectorId,
                String
                    .format(
                        "Exceeded memory limit. New size is %d bytes and max limit is %d bytes",
                        reservedMemoryBytes + requiredBytes,
                        heapLimitBytes
                    )
            );
        }
    }

    /**
     * Whether allocating memory for a model for the detector is allowed
     * @param detectorId Detector Id
     * @param detector Detector config object
     * @param numberOfTrees The number of trees required for the model
     * @return true if allowed; false otherwise
     */
    public synchronized boolean canAllocate(String detectorId, AnomalyDetector detector, int numberOfTrees) {
        long bytes = estimateModelSize(detector, numberOfTrees);
        return totalMemoryBytes + bytes <= heapLimitBytes;
    }

    public synchronized void consumeMemory(long memoryToConsume, boolean reserved, Origin origin) {
        totalMemoryBytes += memoryToConsume;
        adjustOriginMemoryConsumption(memoryToConsume, origin, totalMemoryBytesByOrigin);
        if (reserved) {
            reservedMemoryBytes += memoryToConsume;
            adjustOriginMemoryConsumption(memoryToConsume, origin, reservedMemoryBytesByOrigin);
        }
    }

    private void adjustOriginMemoryConsumption(long memoryToConsume, Origin origin, Map<Origin, Long> mapToUpdate) {
        Long originTotalMemoryBytes = mapToUpdate.getOrDefault(origin, 0L);
        mapToUpdate.put(origin, originTotalMemoryBytes + memoryToConsume);
    }

    public synchronized void releaseMemory(long memoryToShed, boolean reserved, Origin origin) {
        totalMemoryBytes -= memoryToShed;
        adjustOriginMemoryRelease(memoryToShed, origin, totalMemoryBytesByOrigin);
        if (reserved) {
            reservedMemoryBytes -= memoryToShed;
            adjustOriginMemoryRelease(memoryToShed, origin, totalMemoryBytesByOrigin);
        }
    }

    private void adjustOriginMemoryRelease(long memoryToConsume, Origin origin, Map<Origin, Long> mapToUpdate) {
        Long originTotalMemoryBytes = mapToUpdate.getOrDefault(origin, 0L);
        mapToUpdate.put(origin, originTotalMemoryBytes - memoryToConsume);
    }

    /**
     * Gets the estimated size of a RCF model.
     *
     * @param forest RCF configuration
     * @return estimated model size in bytes
     */
    public long estimateModelSize(RandomCutForest forest) {
        return (long) forest.getNumberOfTrees() * (long) forest.getSampleSize() * BOUNDING_BOXES * VECTORS_IN_BOUNDING_BOX * forest
            .getDimensions() * (Long.SIZE / Byte.SIZE);
    }

    /**
     * Gets the estimated size of a RCF model that can be created according to
     * the detector configuration.
     *
     * @param detector detector config object
     * @param numberOfTrees the number of trees in a RCF forest
     * @return estimated model size in bytes
     */
    public long estimateModelSize(AnomalyDetector detector, int numberOfTrees) {
        return rcfSizeConstant * numberOfTrees * detector.getEnabledFeatureIds().size() * detector.getShingleSize();
    }

    /**
     * Bytes to remove to keep AD memory usage within the limit
     * @return bytes to remove
     */
    public long memoryToShed() {
        return totalMemoryBytes - heapLimitBytes;
    }

    /**
     *
     * @return Allowed heap usage in bytes by AD models
     */
    public long getHeapLimit() {
        return heapLimitBytes;
    }

    /**
     *
     * @return Desired model partition size in bytes
     */
    public long getDesiredModelSize() {
        return desiredModelSize;
    }

    /**
     * In case of bugs/errors when allocating/releasing memory, sync used bytes
     * infrequently by recomputing memory usage.
     * @param origin Origin
     * @param totalBytes total bytes from recomputing
     * @param reservedBytes reserved bytes from recomputing
     */
    public synchronized void syncMemoryState(Origin origin, long totalBytes, long reservedBytes) {
        long recordedTotalBytes = totalMemoryBytesByOrigin.getOrDefault(origin, 0L);
        long recordedReservedBytes = reservedMemoryBytesByOrigin.getOrDefault(origin, 0L);
        if (totalBytes == recordedTotalBytes && reservedBytes == recordedReservedBytes) {
            return;
        }
        LOG
            .error(
                String
                    .format(
                        "Memory state do not match.  Recorded: total bytes %d, reserved bytes %d."
                            + "Actual: total bytes %d, reserved bytes: %d",
                        recordedTotalBytes,
                        recordedReservedBytes,
                        totalBytes,
                        reservedBytes
                    )
            );
        // reserved bytes mismatch
        long reservedDiff = reservedBytes - recordedReservedBytes;
        reservedMemoryBytesByOrigin.put(origin, reservedBytes);
        reservedMemoryBytes += reservedDiff;

        long totalDiff = totalBytes - recordedTotalBytes;
        totalMemoryBytesByOrigin.put(origin, totalBytes);
        totalMemoryBytes += totalDiff;
    }
}
