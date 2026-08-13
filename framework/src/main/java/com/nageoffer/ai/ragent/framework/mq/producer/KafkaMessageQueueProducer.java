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

import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nageoffer.ai.ragent.framework.mq.MessageWrapper;
import com.nageoffer.ai.ragent.framework.mq.outbox.KafkaOutboxRelay;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * 基于 Kafka + Outbox 的消息生产者
 * <p>
 * 普通消息直接发送；事务消息在本地事务内写入 {@code t_mq_outbox}，
 * 事务提交后由 {@link KafkaOutboxRelay} 投递到 Kafka，失败定时补偿重试。
 */
@Slf4j
@RequiredArgsConstructor
public class KafkaMessageQueueProducer implements MessageQueueProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaOutboxRelay outboxRelay;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final PlatformTransactionManager transactionManager;

    @Override
    public void send(String topic, String keys, String bizDesc, Object body) {
        keys = StrUtil.isEmpty(keys) ? UUID.randomUUID().toString() : keys;
        String payload = toJson(MessageWrapper.builder().keys(keys).body(body).build());
        try {
            kafkaTemplate.send(topic, keys, payload);
            log.info("[生产者] {} - 已发送 topic: {}, keys: {}", bizDesc, topic, keys);
        } catch (Throwable ex) {
            log.error("[生产者] {} - 消息发送失败，topic: {}, keys: {}", bizDesc, topic, keys, ex);
            throw ex;
        }
    }

    @Override
    public void sendInTransaction(String topic, String keys, String bizDesc, Object body,
                                  Consumer<Object> localTransaction) {
        String resolvedKeys = StrUtil.isEmpty(keys) ? UUID.randomUUID().toString() : keys;
        String outboxId = UUID.randomUUID().toString();

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            writeOutbox(topic, resolvedKeys, body, outboxId, localTransaction);
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    outboxRelay.dispatch(outboxId);
                }
            });
            log.info("[生产者] {} - 事务消息已写入 Outbox（参与现有事务），topic: {}, keys: {}", bizDesc, topic, resolvedKeys);
            return;
        }

        new TransactionTemplate(transactionManager).executeWithoutResult(status ->
                writeOutbox(topic, resolvedKeys, body, outboxId, localTransaction));
        outboxRelay.dispatch(outboxId);
        log.info("[生产者] {} - 事务消息已提交并投递，topic: {}, keys: {}", bizDesc, topic, resolvedKeys);
    }

    /**
     * 同一本地事务内：先执行业务逻辑，再写入 outbox 记录；业务异常则整体回滚、消息不投递
     */
    private void writeOutbox(String topic, String keys, Object body, String outboxId, Consumer<Object> localTransaction) {
        localTransaction.accept(body);
        String payload = toJson(MessageWrapper.builder().keys(keys).body(body).build());
        jdbcTemplate.update("""
                        INSERT INTO t_mq_outbox (id, topic, message_key, body_json, status, retry_count, create_time, update_time)
                        VALUES (?, ?, ?, ?, 0, 0, NOW(), NOW())
                        """,
                outboxId, topic, keys, payload);
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("消息序列化失败", e);
        }
    }
}
