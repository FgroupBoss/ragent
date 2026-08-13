/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.ai.ragent.framework.mq.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nageoffer.ai.ragent.framework.mq.MessageWrapper;
import com.nageoffer.ai.ragent.framework.mq.outbox.KafkaOutboxRelay;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaMessageQueueProducerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final KafkaOutboxRelay outboxRelay = mock(KafkaOutboxRelay.class);

    @Test
    void send_shouldPublishToKafka() {
        KafkaTemplate<String, String> kafkaTemplate = mockKafkaTemplate();
        KafkaMessageQueueProducer producer = new KafkaMessageQueueProducer(
                kafkaTemplate, outboxRelay, objectMapper, jdbcTemplate, mockTransactionManager());

        producer.send("test-topic", "key-1", "单元测试", Map.of("a", 1));

        verify(kafkaTemplate).send(anyString(), anyString(), anyString());
    }

    @Test
    void sendInTransaction_shouldRunLocalTransactionAndWriteOutboxWhenNoActiveTx() {
        KafkaTemplate<String, String> kafkaTemplate = mockKafkaTemplate();
        PlatformTransactionManager txManager = mockTransactionManager();
        KafkaMessageQueueProducer producer = new KafkaMessageQueueProducer(
                kafkaTemplate, outboxRelay, objectMapper, jdbcTemplate, txManager);
        AtomicBoolean executed = new AtomicBoolean(false);

        producer.sendInTransaction("test-topic", "key-2", "单元测试", Map.of("b", 2),
                arg -> executed.set(true));

        assertTrue(executed.get());
        verify(jdbcTemplate).update(anyString(), any(), anyString(), anyString(), anyString());
        verify(outboxRelay).dispatch(anyString());
    }

    @SuppressWarnings("unchecked")
    private KafkaTemplate<String, String> mockKafkaTemplate() {
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        return kafkaTemplate;
    }

    private PlatformTransactionManager mockTransactionManager() {
        PlatformTransactionManager txManager = mock(PlatformTransactionManager.class);
        when(txManager.getTransaction(any()))
                .thenReturn(mock(TransactionStatus.class));
        doNothing().when(txManager).commit(any());
        doNothing().when(txManager).rollback(any());
        return txManager;
    }
}
