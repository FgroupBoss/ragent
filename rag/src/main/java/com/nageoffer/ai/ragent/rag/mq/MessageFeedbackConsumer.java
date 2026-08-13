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

package com.nageoffer.ai.ragent.rag.mq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nageoffer.ai.ragent.framework.mq.MessageWrapper;
import com.nageoffer.ai.ragent.rag.mq.event.MessageFeedbackEvent;
import com.nageoffer.ai.ragent.rag.service.MessageFeedbackService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 消息反馈 MQ 消费者，负责将点赞/点踩事件异步持久化到数据库
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MessageFeedbackConsumer {

    private final MessageFeedbackService feedbackService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "message-feedback_topic${unique-name:}",
            groupId = "message-feedback_cg${unique-name:}")
    public void onMessage(String payload) {
        MessageWrapper<MessageFeedbackEvent> message = parse(payload);
        MessageFeedbackEvent event = message.getBody();

        log.info("[消费者] 开始处理反馈事件，messageId: {}, userId: {}, vote: {}, cancelled: {}, keys: {}",
                event.getMessageId(), event.getUserId(), event.getVote(), event.isCancelled(), message.getKeys());
        feedbackService.submitFeedbackByEvent(event);
    }

    private MessageWrapper<MessageFeedbackEvent> parse(String payload) {
        try {
            return objectMapper.readValue(payload,
                    new TypeReference<MessageWrapper<MessageFeedbackEvent>>() {
                    });
        } catch (Exception e) {
            throw new IllegalArgumentException("消息反馈事件解析失败", e);
        }
    }
}
