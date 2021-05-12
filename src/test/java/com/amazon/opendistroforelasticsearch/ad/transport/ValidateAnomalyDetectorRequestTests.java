/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;
import java.time.Instant;

import org.junit.Test;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESSingleNodeTestCase;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.google.common.collect.ImmutableMap;

public class ValidateAnomalyDetectorRequestTests extends ESSingleNodeTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    @Test
    public void testValidateAnomalyDetectorRequestSerialization() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        TimeValue requestTimeout = new TimeValue(1000L);
        String typeStr = "type";

        ValidateAnomalyDetectorRequest request1 = new ValidateAnomalyDetectorRequest(detector, typeStr, 1, 1, 1, requestTimeout);

        // Test serialization
        BytesStreamOutput output = new BytesStreamOutput();
        request1.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ValidateAnomalyDetectorRequest request2 = new ValidateAnomalyDetectorRequest(input);
        assertEquals("serialization has the wrong detector", request2.getDetector(), detector);
        assertEquals("serialization has the wrong typeStr", request2.getTypeStr(), typeStr);
        assertEquals("serialization has the wrong requestTimeout", request2.getRequestTimeout(), requestTimeout);
    }
}