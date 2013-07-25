package com.blogspot.ilialapitan;

import java.io.File;

/**
 * Service for work with AmazonS3 with Apache Camel help.
 *
 * @author Ilya Lapiatan
 */
public interface CamelService {

    /**
     * Send file to AmazonS3 bucket.
     * @param key  path in bucket to file.
     * @param file file  for sending
     */
    public void send(String key, File file);

    /**
     * Receive file from AmazonS3 bucket.
     * @param key path in bucket to file.
     * @param file file  for receiving
     * @throws Exception
     */
    public void receive(String key, File file) throws Exception;
}
