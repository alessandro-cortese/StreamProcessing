package it.uniroma2.sabd;

import it.uniroma2.sabd.engineering.KafkaProducerApp;
import it.uniroma2.sabd.engineering.KafkaConsumerApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationRunner.class);

    public static void main(String[] args) {
        if (args.length == 0) {
            LOG.error("No arguments provided. Usage: java -jar <jar_name>.jar [producer|consumer]");
            System.exit(1);
        }

        String appToRun = args[0].toLowerCase();

        String[] appSpecificArgs = new String[args.length - 1];
        if (args.length > 1) {
            System.arraycopy(args, 1, appSpecificArgs, 0, args.length - 1);
        }

        switch (appToRun) {
            case "producer":
                LOG.info("Starting Kafka Producer application...");
                KafkaProducerApp.main(appSpecificArgs); // Calls the producer's main method
                break;
            case "consumer":
                LOG.info("Starting Kafka Streams Consumer application...");
                KafkaConsumerApp.main(appSpecificArgs); // Calls the consumer's main method
                break;
            default:
                LOG.error("Unknown application: '{}'. Usage: java -jar <jar_name>.jar [producer|consumer]", appToRun);
                System.exit(1);
        }
    }
}
