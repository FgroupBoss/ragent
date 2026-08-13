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

import java.util.function.Consumer;

/**
 * 消息队列生产者接口
 */
public interface MessageQueueProducer {

    /**
     * 发送消息
     *
     * @param topic   目标 topic
     * @param keys    业务 key，可用于幂等判断
     * @param bizDesc 业务描述，用于日志标识
     * @param body    业务载荷
     * 发送普通消息（Kafka 异步发送，发送失败抛出异常）
     */
    void send(String topic, String keys, String bizDesc, Object body);

    /**
     * 发送事务消息
     * <p>
     * 流程：发送 half 消息 → 执行本地事务 → 根据结果 commit/rollback
     * <p>
     * 事务消息（Kafka Outbox 模式）：
     * 在本地事务内执行业务逻辑并写入 outbox 表，事务提交后投递到 Kafka；
     * 投递失败由定时任务补偿重试，消费端需保证幂等
     *
     * @param topic            目标 topic
     * @param keys             业务 key
     * @param bizDesc          业务描述
     * @param body             业务载荷
     * @param localTransaction 本地事务逻辑，在 half 消息发送成功后执行；抛异常则回滚消息
     */
    void sendInTransaction(String topic, String keys, String bizDesc, Object body,
                           Consumer<Object> localTransaction);
}
