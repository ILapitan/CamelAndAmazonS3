package com.blogspot.ilialapitan;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.aws.s3.S3Constants;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.SimpleRegistry;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

/**
 * Service for work with AmazonS3 with Apache Camel help.
 * This is class configured by Java DSL.
 *
 * @author Ilya Lapitan
 */
public final class CamelServiceJavaDSL implements CamelService {
    private static final String BUCKET =  "[bucket_name]";

    private static int END_STREAM = -1;//marker of stream end
    private static int BUFFER_SIZE = 1024 * 100;//set up buffer size (100Kb)

    private SimpleRegistry registry;
    private AmazonS3Client amazonS3Client;
    private CamelContext context;
    private ProducerTemplate producer;
    private ConsumerTemplate consumer;

    public  CamelServiceJavaDSL() throws Exception {
        registry = new SimpleRegistry();
        amazonS3Client = new AmazonS3Client(new BasicAWSCredentials("[access_key]","[secret_key]"));

        //add default amazon client to camel context
        registry.put("client", amazonS3Client);

        //create camel context, producer and consumer
        context = new DefaultCamelContext(registry);
        producer = context.createProducerTemplate();
        consumer = context.createConsumerTemplate();

        //manually starts context
        context.start();
    }


    /**
     * Send file to AmazonS3 bucket.
     * @param key  path in bucket to file.
     * @param file file  for sending
     */
    @Override
    public void send(final String key, final File file) {
        producer.send(makeEndpointURI(BUCKET), ExchangePattern.InOnly, new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                exchange.getIn().setHeader(S3Constants.KEY, key + File.separator + file.getName());
                exchange.getIn().setBody(file);
            }
        });
    }

    /**
     * Receive file from AmazonS3 bucket.
     * @param key path in bucket to file.
     * @param file file  for receiving
     * @throws Exception
     */
    @Override
    public void receive(final String key,final File file) throws Exception {
        try(OutputStream stream = new BufferedOutputStream(new FileOutputStream(file));
            S3ObjectInputStream s3ObjectInputStream = (S3ObjectInputStream) consumer.receiveBody(makeEndpointURI(BUCKET, key, file));
        ) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while((bytesRead = s3ObjectInputStream.read(buffer)) != END_STREAM){
                stream.write(buffer, 0, bytesRead);
            }
        }
    }

    //make endpoint uri for amazon s3 by bucket name
    private String makeEndpointURI(final String bucket){
        return "aws-s3://" + bucket +  "?amazonS3Client=#client";
    }

    //add prefix param for consumer
    private String addKeyPrefix(final String key, final File file){
        return "&prefix=" + key + File.separator + file.getName();
    }

    //make endpoint by bucket, key and file
    private String makeEndpointURI(final String bucket, final String key, final File file){
        return makeEndpointURI(bucket) + addKeyPrefix(key, file);
    }
}
