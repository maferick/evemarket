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

session_start();

// Ensure the CSRF token exists *before* we release the session lock so
// subsequent requests on this session can read it without needing a
// write-reopen round trip.
if (!isset($_SESSION['csrf_token'])) {
    $_SESSION['csrf_token'] = bin2hex(random_bytes(32));
}

// Release the session file lock immediately. PHP's default (files) session
// handler holds an exclusive flock() for the entire request lifetime, which
// serialises every request that shares a session cookie — meaning a second
// tab can't load a page while the first one is still rendering. $_SESSION
// stays readable in-memory after this call; any code that needs to *write*
// to the session must wrap the mutation in supplycore_session_write().
session_write_close();

require_once __DIR__ . '/functions.php';
