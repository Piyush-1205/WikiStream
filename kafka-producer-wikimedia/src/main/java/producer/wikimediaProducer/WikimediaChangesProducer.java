
package producer.wikimediaProducer;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import jakarta.annotation.PreDestroy;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import producer.WikimediaChangesHandler;
import com.launchdarkly.eventsource.ConnectStrategy;

import java.net.URI;

@Service
public class WikimediaChangesProducer {

    private static final String TOPIC = "wikimedia_recentchange";
    private static final String URL =
            "https://stream.wikimedia.org/v2/stream/recentchange";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private BackgroundEventSource backgroundEventSource;

    public WikimediaChangesProducer(
            KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() throws StreamException {

        BackgroundEventHandler handler =
                new WikimediaChangesHandler(kafkaTemplate, TOPIC);

        EventSource.Builder eventSourceBuilder =
                new EventSource.Builder(
                        ConnectStrategy.http(URI.create(URL))
                                .header(
                                        "User-Agent",
                                        "WikiStream/0.0.1 (https://github.com/Piyush-1205/WikiStream)"
                                )
                );

        backgroundEventSource =
                new BackgroundEventSource.Builder(
                        handler, eventSourceBuilder
                ).build();

        backgroundEventSource.start();
    }

    @PreDestroy
    public void shutdown() {
        if (backgroundEventSource != null) {
            backgroundEventSource.close();
        }
    }
}
