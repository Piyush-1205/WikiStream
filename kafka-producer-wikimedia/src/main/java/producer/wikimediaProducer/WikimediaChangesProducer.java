package producer.wikimediaProducer;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import producer.WikimediaChangesHandler;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class WikimediaChangesProducer {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(WikimediaChangesProducer.class);

    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage() throws InterruptedException, StreamException {

        String topic = "wikimedia_recentchange";

        BackgroundEventHandler handler =
                new WikimediaChangesHandler(kafkaTemplate, topic);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        // new way of doing this (the lecture uses a older way of doing it , with older api)

        // First, configure the SSE connection
        EventSource.Builder eventSourceBuilder =
                new EventSource.Builder(URI.create(url));

        // Then wrap it with BackgroundEventSource
        BackgroundEventSource backgroundEventSource =
                new BackgroundEventSource.Builder(handler, eventSourceBuilder)
                        .build();

        // Start receiving events
        backgroundEventSource.start();

//        TimeUnit.MINUTES.sleep(10);

        backgroundEventSource.close();
    }
}
