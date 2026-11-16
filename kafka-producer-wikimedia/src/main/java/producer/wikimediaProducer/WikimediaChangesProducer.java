package producer.wikimediaProducer;

import lombok.extern.flogger.Flogger;
import org.slf4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class WikimediaChangesProducer {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(WikimediaChangesProducer.class);

    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message){
        logger.info("Producing message: " + message);

        kafkaTemplate.send("wikimedia_recentchange", message);
    }
}
