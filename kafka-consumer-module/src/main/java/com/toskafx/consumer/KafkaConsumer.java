package com.toskafx.consumer;

import com.toskafx.data.WikimediaData;
import com.toskafx.data.WikimediaRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaConsumer {

    private final WikimediaRepository wikimediaRepository;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = {"wikimedia-recent-changes"}, groupId = "myGroup")
    public void consumeEventMessages(String eventMessage){
        LOG.info("Event message received -> {}", eventMessage);
        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setEventMessage(eventMessage);
        wikimediaRepository.save(wikimediaData);

    }
}
