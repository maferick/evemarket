<?php

/**
 * SupplyCore Public Proxy — Configuration
 *
 * Copy this file to config.php and fill in the values.
 * Generate an API key pair in your SupplyCore instance first.
 *
 * For the ESI login feature: create a new application at
 * https://developers.eveonline.com/applications with scope "publicData"
 * and callback URL pointing to /callback on this proxy domain.
 */

return [
    // ── SupplyCore backend (required) ───────────────────────────────
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

    // ── ESI SSO login (optional) ────────────────────────────────────
    // Register an app at https://developers.eveonline.com/applications
    // Scopes: publicData   |   Callback: https://your-proxy/callback
    'esi_client_id'     => '',
    'esi_client_secret' => '',
    'esi_callback_url'  => 'https://your-proxy.example.com/callback',

    // ── Privacy / IP hashing (required if ESI login is enabled) ────
    // 64-byte hex secret used to pepper IP hashes so raw addresses are
    // never stored. Generate with:
    //     php -r "echo bin2hex(random_bytes(32));"
    // NEVER rotate this in production — rotating it invalidates all
    // existing hashes and breaks cross-matching.
    'ip_hash_salt' => '',

    // Where the SQLite session database lives (relative to lib/).
    // Make sure the web server user can write to the parent directory.
    'session_db_path' => __DIR__ . '/../storage/session.sqlite',

    // Set to true if the proxy sits behind a trusted reverse proxy /
    // load balancer so client IPs come in as X-Forwarded-For.
    'trust_forwarded_for' => false,

    // Optional contact line rendered on privacy.php
    'privacy_contact' => '',
];
