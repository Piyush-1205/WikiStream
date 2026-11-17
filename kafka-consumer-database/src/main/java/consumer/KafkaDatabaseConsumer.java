package consumer;

import consumer.entity.WikimediaData;
import consumer.repository.WikimediaDataRepo;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    @Autowired
    private WikimediaDataRepo wikimediaDataRepo;

    @KafkaListener(topics = "wikimedia_recentchange", groupId = "myGroup")
    public void consumer(String eventMessage) {
        logger.info(String.format("Message received -> %s", eventMessage));

        WikimediaData data = new WikimediaData();
        data.setWikiEventData(eventMessage);
        wikimediaDataRepo.save(data);
    }
}
