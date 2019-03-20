package com.intel.cosbench.client.S3Stor;

public interface S3Constants {
    // --------------------------------------------------------------------------
    // CONNECTION
    // --------------------------------------------------------------------------

    String CONN_TIMEOUT_KEY = "timeout";
    int CONN_TIMEOUT_DEFAULT = 30000;
    // --------------------------------------------------------------------------
    // ENDPOINT
    // --------------------------------------------------------------------------
    String ENDPOINT_KEY = "endpoint";
    String ENDPOINT_DEFAULT = "http://s3.amazonaws.com";

    // --------------------------------------------------------------------------
    // AUTHENTICATION
    // --------------------------------------------------------------------------

    String AUTH_USERNAME_KEY = "accessKey";
    String AUTH_USERNAME_DEFAULT = "";
    
    String AUTH_PASSWORD_KEY = "secretKey";
    String AUTH_PASSWORD_DEFAULT = "";
    
    // --------------------------------------------------------------------------
    // CLIENT CONFIGURATION
    // --------------------------------------------------------------------------
    String PROXY_HOST_KEY = "proxyHost";
    String PROXY_PORT_KEY = "proxyPort";
       
    // --------------------------------------------------------------------------
    // SIGNER CONFIGURATION
    // --------------------------------------------------------------------------
    String SIGNER_OVERRIDE_KEY = "signerOverride";
    String SIGNER_OVERRIDE_DEFAULT = "AWS4SignerType";
    
    // --------------------------------------------------------------------------
    // CERTIFICATE CHECK
    // --------------------------------------------------------------------------
    String NO_CERT_CHECK_KEY = "insecure";
    boolean NO_CERT_CHECK_DEFAULT = false;
    
    // --------------------------------------------------------------------------
    // HASH CHECK
    // --------------------------------------------------------------------------
    String HASH_CHECK_KEY = "hashCheck";
    boolean HASH_CHECK_DEFAULT = false;
        
    // --------------------------------------------------------------------------
    // PATH STYLE ACCESS
    // --------------------------------------------------------------------------
    String PATH_STYLE_ACCESS_KEY = "pathStyleAccess";
    boolean PATH_STYLE_ACCESS_DEFAULT = false;

    // --------------------------------------------------------------------------
    // CONTEXT NEEDS FROM AUTH MODULE
    // --------------------------------------------------------------------------
    String S3CLIENT_KEY = "s3client";

}
