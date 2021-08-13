package com.alperbasak.kafkaserver.controller;

import com.alperbasak.CaseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@CrossOrigin
@RestController
public class CaseEventController {
    private final Logger logger = LoggerFactory.getLogger(CaseEventController.class);

    final
    KafkaTemplate<Void, Object> kafkaTemplate;

    final String topic = "cases";

    public CaseEventController(KafkaTemplate<Void, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping(value = "/case")
    CaseEvent addCase(@RequestBody CaseEvent caseEvent) {
        ListenableFuture<SendResult<Void, Object>> future =
                kafkaTemplate.send(topic, caseEvent);

        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<Void, Object> result) {
                logger.info("Delivered {} with offset {}", caseEvent, result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                logger.error("Unable to deliver {} {}", caseEvent, ex.getMessage());
            }
        });
        return caseEvent;
    }
}
