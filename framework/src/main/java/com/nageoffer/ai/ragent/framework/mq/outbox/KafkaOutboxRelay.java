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

package com.nageoffer.ai.ragent.framework.mq.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Kafka Outbox 中继器
 * <p>
 * 事务消息替代方案：本地事务内写 {@code t_mq_outbox}，事务提交后投递到 Kafka；
 * 投递失败进入 PENDING/FAILED，由定时任务按退避策略重试，消费端 at-least-once + 幂等。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaOutboxRelay {

    private static final int MAX_RETRY_COUNT = 10;
    private static final long RETRY_BACKOFF_SECONDS = 10;
    private static final int BATCH_SIZE = 100;
    private static final int SEND_TIMEOUT_SECONDS = 5;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;

    /**
     * 投递单条 outbox 记录（事务提交后调用）
     */
    public void dispatch(String id) {
        try {
            OutboxRow row = findById(id);
            if (row == null || row.status() == 1) {
                return;
            }
            sendAndMark(row);
        } catch (Exception e) {
            log.error("[Outbox] 投递失败 id={}", id, e);
            markFailed(id);
        }
    }

    /**
     * 定时扫描未投递记录，按退避策略重试
     */
    @Scheduled(fixedDelayString = "${ragent.mq.outbox.poll-interval-ms:5000}")
    public void pollAndDispatch() {
        List<OutboxRow> rows;
        try {
            rows = jdbcTemplate.query("""
                            SELECT id, topic, message_key, body_json, status
                            FROM t_mq_outbox
                            WHERE status IN (0, 2) AND retry_count < ?
                              AND (next_retry_time IS NULL OR next_retry_time <= NOW())
                            ORDER BY create_time
                            LIMIT ?
                            """,
                    (rs, i) -> new OutboxRow(
                            rs.getString("id"),
                            rs.getString("topic"),
                            rs.getString("message_key"),
                            rs.getString("body_json"),
                            rs.getInt("status")),
                    MAX_RETRY_COUNT, BATCH_SIZE);
        } catch (Exception e) {
            log.error("[Outbox] 扫描待投递消息失败", e);
            return;
        }

        for (OutboxRow row : rows) {
            try {
                sendAndMark(row);
            } catch (Exception e) {
                log.error("[Outbox] 定时重试投递失败 id={}, topic={}", row.id(), row.topic(), e);
                markFailed(row.id());
            }
        }
    }

    private OutboxRow findById(String id) {
        List<OutboxRow> rows = jdbcTemplate.query(
                "SELECT id, topic, message_key, body_json, status FROM t_mq_outbox WHERE id = ?",
                (rs, i) -> new OutboxRow(
                        rs.getString("id"),
                        rs.getString("topic"),
                        rs.getString("message_key"),
                        rs.getString("body_json"),
                        rs.getInt("status")),
                id);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private void sendAndMark(OutboxRow row) throws Exception {
        kafkaTemplate.send(row.topic(), row.messageKey(), row.bodyJson())
                .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        int updated = jdbcTemplate.update(
                "UPDATE t_mq_outbox SET status = 1, update_time = NOW() WHERE id = ? AND status <> 1",
                row.id());
        log.info("[Outbox] 消息已投递 topic={}, keys={}, id={}", row.topic(), row.messageKey(), row.id());
        if (updated == 0) {
            log.warn("[Outbox] 投递成功但状态更新异常 id={}", row.id());
        }
    }

    private void markFailed(String id) {
        jdbcTemplate.update(
                "UPDATE t_mq_outbox SET status = 2, retry_count = retry_count + 1, "
                        + "next_retry_time = DATE_ADD(NOW(), INTERVAL ? SECOND), update_time = NOW() WHERE id = ?",
                RETRY_BACKOFF_SECONDS, id);
    }

    private record OutboxRow(String id, String topic, String messageKey, String bodyJson, int status) {
    }
}
