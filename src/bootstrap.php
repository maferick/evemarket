<?php

declare(strict_types=1);

// Block known AI crawlers before any expensive work (session, DB, etc.).
// Primary blocking should be done at the nginx level via
// setup/nginx-block-ai-crawlers.conf — this is a defense-in-depth fallback.
if (preg_match('/ClaudeBot|GPTBot|CCBot|anthropic-ai|ChatGPT-User|Bytespider|PetalBot|Google-Extended/i', $_SERVER['HTTP_USER_AGENT'] ?? '')) {
    http_response_code(403);
    header('Content-Type: text/plain');
    exit('Forbidden');
}

// Load app helpers (config, Redis client, etc.) *before* touching sessions so
// supplycore_register_session_handler() can swap the default files handler
// for the Redis-backed one when Redis is available.
require_once __DIR__ . '/functions.php';
require_once __DIR__ . '/session.php';

supplycore_register_session_handler();

session_start();

// Ensure the CSRF token exists *before* we release the session lock so
// subsequent requests on this session can read it without needing a
// write-reopen round trip.
if (!isset($_SESSION['csrf_token'])) {
    $_SESSION['csrf_token'] = bin2hex(random_bytes(32));
}

// Release the session lock immediately. With the default (files) handler PHP
// holds an exclusive flock() for the entire request lifetime, which serialises
// every request that shares a session cookie — meaning a second tab can't
// load a page while the first one is still rendering. With the Redis handler
// registered above the lock is per-key in-memory, but we still close early so
// any write window is as small as possible. $_SESSION stays readable
// in-memory after this call; any code that needs to *write* to the session
// must wrap the mutation in supplycore_session_write().
session_write_close();
