package com.fastcampus.clickstream;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

// Runnable 인터페이스를 구현하고 있으며, 각 LogGenerator 인스턴스는 별도의 스레드에서 실행됩니다. 웹 로그를 생성하고 이를 Kafka 토픽으로 전송하는 역할을 합니다.
public class LogGenerator implements Runnable {
    private CountDownLatch latch; // CountDownLatch 객체로, 이 객체를 통해 모든 로그 생성 스레드가 완료될 때까지 기다릴 수 있습니다
    private String ipAddr; // 생성할 로그에 사용할 IP 주소.
    private String sessionId; // 사용자 세션 ID, 로그에 포함될 세션 식별자.
    private int durationSeconds; // 로그 생성이 지속될 시간(초 단위).
    private Random rand;

    private final static long MINIMUM_SLEEP_TIME = 500;
    private final static long MAXIMUM_SLEEP_TIME = 60 * 1000;

    private final static String TOPIC_NAME = "weblog";

    public LogGenerator(CountDownLatch latch, String ipAddr, String sessionId, int durationSeconds) {
        this.latch = latch;
        this.ipAddr = ipAddr;
        this.sessionId = sessionId;
        this.durationSeconds = durationSeconds;
        this.rand = new Random(); // 랜덤 값을 생성하기 위한 Random 객체로, 로그의 여러 필드를 랜덤하게 생성합니다.
    }

    @Override
    public void run() { // 이 메서드는 스레드가 실행될 때 호출되며, 로그 생성의 시작을 콘솔에 출력합니다.
        System.out.println("Starting log generator (ipAddr=" + ipAddr + ", sessionId=" + sessionId + ", durationSeconds=" + durationSeconds);

        Properties props = new Properties(); // Properties 객체를 사용하여 Kafka 프로듀서의 설정을 정의합니다
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "LogGenerator");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Kafka에서 메시지를 직렬화하는 방법을 설정. 이 예제에서는 StringSerializer를 사용합니다.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Kafka에서 메시지를 직렬화하는 방법을 설정. 이 예제에서는 StringSerializer를 사용합니다.

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        long startTime = System.currentTimeMillis(); // startTime을 기록

        while (isDuration(startTime)) { // while 루프를 통해 지정된 durationSeconds 동안 로그를 생성합니다.
            long sleepTime = MINIMUM_SLEEP_TIME + Double.valueOf(rand.nextDouble() * (MAXIMUM_SLEEP_TIME - MINIMUM_SLEEP_TIME)).longValue();
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            String responseCode = getResponseCode();
            String responseTime = getResponseTime();
            String method = getMethod();
            String url = getUrl();
            OffsetDateTime offsetDataTime = OffsetDateTime.now(ZoneId.of("UTC"));

            String log = String.format("%s %s %s %s %s %s %s",
                    ipAddr, offsetDataTime, method, url, responseCode, responseTime, sessionId);
            System.out.println(log);

            // 로그를 생성할 때마다 ProducerRecord를 생성하여 producer.send() 메서드로 전송합니다.
            // 각 로그는 랜덤한 HTTP 응답 코드, 응답 시간, HTTP 메소드, URL 등을 포함하며, 이를 Kafka weblog 토픽으로 전송합니다.
            producer.send(new ProducerRecord<>(TOPIC_NAME, log)); 
        }
        producer.close(); // 로그 생성이 완료되면 Kafka 프로듀서를 종료

        System.out.println("Stopping log generator (ipAddr=" + ipAddr + ", sessionId=" + sessionId + ", durationSeconds=" + durationSeconds);

        this.latch.countDown(); // latch.countDown()을 호출하여 카운트다운 래치를 감소시킵니다. 이로써 다른 스레드들이 종료되었음을 알 수 있습니다.
    }

    private boolean isDuration(long startTime) { // 로그 생성이 주어진 시간(durationSeconds) 동안 계속될지 여부를 확인하는 메서드입니다.
        return System.currentTimeMillis() - startTime < durationSeconds * 1000L;
    }

    private String getResponseCode() { // 응답 코드를 랜덤하게 생성합니다. 97% 확률로 "200" 응답을 반환하고, 나머지 3% 확률로 "404" 응답을 반환합니다.
        String responseCode = "200";
        if (rand.nextDouble() > 0.97) {
            responseCode = "404";
        }
        return responseCode;
    }

    private String getResponseTime() { // 응답 시간을 랜덤하게 생성합니다. 100ms에서 1000ms 사이의 값을 생성합니다.
        int responseTime = 100 + rand.nextInt(901);
        return String.valueOf(responseTime);
    }

    private String getMethod() { // HTTP 메소드("GET" 또는 "POST")를 랜덤하게 선택합니다. 70% 확률로 "GET", 나머지 30% 확률로 "POST"를 선택합니다.
        if (rand.nextDouble() > 0.7) {
            return "POST";
        } else {
            return "GET";
        }
    }

    private String getUrl() { // 요청 URL을 랜덤하게 생성합니다. 90% 확률로 "/" (홈페이지), 10% 확률로 "/ads/page" 또는 "/doc/page" 경로를 생성합니다.
        double randomValue = rand.nextDouble();
        if (randomValue > 0.9) {
            return "/ads/page";
        } else if (randomValue > 0.8) {
            return "/doc/page";
        } else {
            return "/";
        }
    }
}
