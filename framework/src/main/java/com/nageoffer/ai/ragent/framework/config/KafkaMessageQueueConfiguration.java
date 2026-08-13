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

package com.nageoffer.ai.ragent.framework.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nageoffer.ai.ragent.framework.mq.outbox.KafkaOutboxRelay;
import com.nageoffer.ai.ragent.framework.mq.producer.KafkaMessageQueueProducer;
import com.nageoffer.ai.ragent.framework.mq.producer.MessageQueueProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;

/**
 * Kafka 消息队列自动装配配置
 * <p>
 * 提供类型化的 {@link KafkaTemplate<String,String>}（String 序列化，
 * 由 {@code spring.kafka.producer.*} 配置驱动），并装配
 * {@link MessageQueueProducer}（Outbox 事务消息）。
 */
@Configuration
public class KafkaMessageQueueConfiguration {

    @Bean
    public ProducerFactory<String, String> kafkaProducerFactory(KafkaProperties properties) {
        Map<String, Object> configs = properties.buildProducerProperties();
        // 强制使用 String 序列化，避免依赖 spring.kafka.producer 缺省类型
        configs.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        configs.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configs);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public MessageQueueProducer messageQueueProducer(KafkaTemplate<String, String> kafkaTemplate,
                                                     KafkaOutboxRelay outboxRelay,
                                                     ObjectProvider<ObjectMapper> objectMapperProvider,
                                                     JdbcTemplate jdbcTemplate,
                                                     PlatformTransactionManager transactionManager) {
        ObjectMapper objectMapper = objectMapperProvider.getIfAvailable(ObjectMapper::new);
        return new KafkaMessageQueueProducer(kafkaTemplate, outboxRelay, objectMapper, jdbcTemplate, transactionManager);
    }
}
