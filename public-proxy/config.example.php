<?php

/**
 * SupplyCore Public Proxy — Configuration
 *
 * Copy this file to config.php and fill in the values.
 * Generate an API key pair in your SupplyCore instance first.
 */

return [
    // Base URL of your SupplyCore instance (no trailing slash)
    'supplycore_url' => 'https://supplycore.example.com',

    // API key ID (starts with pk_)
    'api_key_id' => 'pk_your_key_id_here',

    // API shared secret (64-char hex string)
    'api_secret' => 'your_shared_secret_here',

    // Request timeout in seconds
    'timeout' => 15,

    // Site branding shown in the proxy header
    'site_name' => 'Battle Reports',
];
