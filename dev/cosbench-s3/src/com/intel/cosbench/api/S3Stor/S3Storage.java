package com.intel.cosbench.api.S3Stor;

import static com.intel.cosbench.client.S3Stor.S3Constants.*;

import java.io.*;

import org.apache.http.HttpStatus;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

import com.intel.cosbench.api.storage.*;
import com.intel.cosbench.api.context.*;
import com.intel.cosbench.config.Config;
import com.intel.cosbench.log.Logger;

public class S3Storage extends NoneStorage {
	private int timeout;
	
    private String accessKey;
    private String secretKey;
    private String endpoint;
    
    private AmazonS3 client;

    @Override
    public void init(Config config, Logger logger) {
    	super.init(config, logger);
    	
    	timeout = config.getInt(CONN_TIMEOUT_KEY, CONN_TIMEOUT_DEFAULT);

    	parms.put(CONN_TIMEOUT_KEY, timeout);
    	
    	endpoint = config.get(ENDPOINT_KEY, ENDPOINT_DEFAULT);
        accessKey = config.get(AUTH_USERNAME_KEY, AUTH_USERNAME_DEFAULT);
        secretKey = config.get(AUTH_PASSWORD_KEY, AUTH_PASSWORD_DEFAULT);

        boolean pathStyleAccess = config.getBoolean(PATH_STYLE_ACCESS_KEY, PATH_STYLE_ACCESS_DEFAULT);
        
		String proxyHost = config.get(PROXY_HOST_KEY, "");
		String proxyPort = config.get(PROXY_PORT_KEY, "");
        
        parms.put(ENDPOINT_KEY, endpoint);
    	parms.put(AUTH_USERNAME_KEY, accessKey);
    	parms.put(AUTH_PASSWORD_KEY, secretKey);
    	parms.put(PATH_STYLE_ACCESS_KEY, pathStyleAccess);
    	parms.put(PROXY_HOST_KEY, proxyHost);
    	parms.put(PROXY_PORT_KEY, proxyPort);
        initClient();
    }

    private AmazonS3 initClient() {
        logger.debug("initialize S3 client with storage config: {}", parms);
        String proxyHost = parms.getStr(PROXY_HOST_KEY);
        String proxyPort = parms.getStr(PROXY_PORT_KEY);
        //String signerOverride = parms.getStr(SIGNER_OVERRIDE_KEY);
        String signerType = getSignerType(parms.getStr(SIGNER_OVERRIDE_KEY));

        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setConnectionTimeout(parms.getInt(CONN_TIMEOUT_KEY));
        if ((!proxyHost.equals("")) && (!proxyPort.equals(""))) {
            clientConf.setProxyHost(proxyHost);
            clientConf.setProxyPort(Integer.parseInt(proxyPort));
        }
        clientConf.withUseExpectContinue(false);
        clientConf.withSignerOverride(signerType);

        AWSCredentials myCredentials = new BasicAWSCredentials(accessKey, secretKey);
        client = new AmazonS3Client(myCredentials, clientConf);
        client.setEndpoint(endpoint);
        client.setS3ClientOptions(new S3ClientOptions()
                .withPathStyleAccess(parms.getBoolean(PATH_STYLE_ACCESS_KEY)));

        logger.debug("S3 Client has been intitialized");
        return client;
    }

    private String getSignerType(String signerOverride) {
    	String signerType = "";
    	
    	// Configure Signer Type
        if (signerOverride.isEmpty()) {
        	signerType = "AWS4SignerType";
        } else if (signerOverride.equalsIgnoreCase("V3")){
        	signerType = "AWS3SignerType";
        } else if (signerOverride.equalsIgnoreCase("V4")){
        	signerType = "AWS4SignerType";
        } else if (signerOverride.equalsIgnorecase("S3V4")) {
        	signerType = "AWSS3V4SignerType";
        } else if (signerOverride.equalsIgnoreCase("NoOp")){
        	signerType = "NoOpSignerType";
        } else if (signerOverride.equalsIgnoreCase("V4Unsigned")){
        	signerType = "AWS4UnsignedPayloadSignerType";
        } else {
        	signerType = "AWS4SignerType";
        }
        return signerType;
    }

    
    @Override
    public void setAuthContext(AuthContext info) {
        super.setAuthContext(info);
//        try {
//        	client = (AmazonS3)info.get(S3CLIENT_KEY);
//            logger.debug("s3client=" + client);
//        } catch (Exception e) {
//            throw new StorageException(e);
//        }
    }

    @Override
    public void dispose() {
        super.dispose();
        client = null;
    }

	@Override
    public InputStream getObject(String container, String object, Config config) {
        super.getObject(container, object, config);
        InputStream stream;
        try {
        	
            S3Object s3Obj = client.getObject(container, object);
            stream = s3Obj.getObjectContent();
            
        } catch (Exception e) {
            throw new StorageException(e);
        }
        return stream;
    }

    @Override
    public void createContainer(String container, Config config) {
        super.createContainer(container, config);
        try {
        	if(!client.doesBucketExist(container)) {
	        	
	            client.createBucket(container);
        	}
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

	@Override
    public void createObject(String container, String object, InputStream data,
            long length, Config config) {
        super.createObject(container, object, data, length, config);
        try {
    		ObjectMetadata metadata = new ObjectMetadata();
    		metadata.setContentLength(length);
    		metadata.setContentType("application/octet-stream");
    		
        	client.putObject(container, object, data, metadata);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void deleteContainer(String container, Config config) {
        super.deleteContainer(container, config);
        try {
        	if(client.doesBucketExist(container)) {
        		client.deleteBucket(container);
        	}
        } catch(AmazonS3Exception awse) {
        	if(awse.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
        		throw new StorageException(awse);
        	}
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void deleteObject(String container, String object, Config config) {
        super.deleteObject(container, object, config);
        try {
            client.deleteObject(container, object);
        } catch(AmazonS3Exception awse) {
        	if(awse.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
        		throw new StorageException(awse);
        	}
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

}
