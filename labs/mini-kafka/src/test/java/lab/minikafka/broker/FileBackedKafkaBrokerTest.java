package lab.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import lab.minikafka.api.FetchResult;
import lab.minikafka.api.MiniKafkaBroker;
import lab.minikafka.model.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileBackedKafkaBrokerTest {

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
        System.setProperty("org.slf4j.simpleLogger.showShortLogName", "true");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "false");
    }

    @TempDir
    Path tempDir;

    @Test
    void recordsSurviveBrokerRestart() {
        MiniKafkaBroker writer = new FileBackedKafkaBroker(tempDir);
        writer.createTopic("orders", 1);
        writer.append("orders", 0, bytes("order-1"), bytes("created"));
        writer.append("orders", 0, bytes("order-2"), bytes("paid"));

        MiniKafkaBroker reader = new FileBackedKafkaBroker(tempDir);
        reader.createTopic("orders", 1);

        FetchResult result = reader.fetch("orders", 0, 0, 10);
        List<Message> messages = result.messages();

        assertEquals(2, messages.size());
        assertEquals(2L, reader.endOffset("orders", 0));
        assertArrayEquals(bytes("order-1"), messages.get(0).key());
        assertArrayEquals(bytes("created"), messages.get(0).value());
        assertArrayEquals(bytes("order-2"), messages.get(1).key());
        assertArrayEquals(bytes("paid"), messages.get(1).value());
    }

    @Test
    void appendContinuesFromRecoveredEndOffset() {
        MiniKafkaBroker firstBroker = new FileBackedKafkaBroker(tempDir);
        firstBroker.createTopic("orders", 1);
        firstBroker.append("orders", 0, bytes("order-1"), bytes("created"));

        MiniKafkaBroker restartedBroker = new FileBackedKafkaBroker(tempDir);
        restartedBroker.createTopic("orders", 1);

        long nextOffset = restartedBroker.append("orders", 0, bytes("order-2"), bytes("paid"));
        FetchResult result = restartedBroker.fetch("orders", 0, 0, 10);

        assertEquals(1L, nextOffset);
        assertEquals(2L, restartedBroker.endOffset("orders", 0));
        assertEquals(2, result.messages().size());
        assertEquals(1L, result.messages().get(1).offset());
        assertArrayEquals(bytes("order-2"), result.messages().get(1).key());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
