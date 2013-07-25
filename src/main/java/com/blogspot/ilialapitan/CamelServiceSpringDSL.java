package com.blogspot.ilialapitan;

import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.EndpointInject;
import org.apache.camel.ProducerTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;


/**
 * Service for work with AmazonS3 with Apache Camel help.
 * This is class configured by Spring DSL.
 *
 * @author Ilya Lapitan
 */
@Service("camelService")
public final class CamelServiceSpringDSL implements CamelService {
    private static final String IN_ENDPOINT = "direct:send";
    private static final String OUT_ENDPOINT = "aws-s3://[bucket_name]?amazonS3Client=#amazonClient&deleteAfterRead=false&prefix=";

    private static int END_STREAM = -1;//marker of stream end
    private static int BUFFER_SIZE = 1024 * 100;//set up buffer size (100Kb)

    @Resource
    private CamelContext camelContext;

    @EndpointInject
    private ConsumerTemplate consumer;

    @EndpointInject
    private ProducerTemplate producer;

    private String key = "";//it can't be null

    /**
     * Send file to AmazonS3 bucket.
     * @param key  path in bucket to file.
     * @param file file  for sending
     */
    @Override
    public void send(final String key,final File file) {
        setKey(key + File.separator + file.getName());
        producer.sendBody(IN_ENDPOINT, file);
    }

    /**
     * Receive file from AmazonS3 bucket.
     * @param key path in bucket to file.
     * @param file file  for receiving
     * @throws Exception
     */
    @Override
    public void receive(final String key,final File file) throws Exception {
        setKey(key + File.separator + file.getName());
        try(OutputStream stream = new BufferedOutputStream(new FileOutputStream(file));
            S3ObjectInputStream s3ObjectInputStream = (S3ObjectInputStream) consumer.receiveBody(OUT_ENDPOINT + getKey());
        ) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while((bytesRead = s3ObjectInputStream.read(buffer)) != END_STREAM){
                stream.write(buffer, 0, bytesRead);
            }
        }
    }

    /**
     * Get key for file.
     *
     * @return  String key.
     */
    public String getKey(){
        return key;  //key - path to file into amazon s3 bucket
    }

    //set key for file
    private void setKey(String key){
        this.key = key;
    }
}
