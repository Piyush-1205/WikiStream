package producer;


import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.slf4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;

public class WikimediaChangesHandler implements BackgroundEventHandler {

    private static Logger logger = org.slf4j.LoggerFactory.getLogger(WikimediaChangesHandler.class);


    private KafkaTemplate<String, String> kafkaTemplate;
    private String topic;

    public WikimediaChangesHandler(KafkaTemplate<String, String> kafkaTemplate, String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        logger.info("========== WIKIMEDIA CONNECTION OPENED ==========");
    }

    @Override
    public void onClosed() throws Exception {

    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) {
        String data = messageEvent.getData();

//        logger.info("Event data -> {}", data);

        kafkaTemplate.send(topic, data)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to publish event", exception);
                    }
                });
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("========== WIKIMEDIA CONNECTION ERROR ==========",
                throwable);
    }
}
