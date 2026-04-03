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

require_once __DIR__ . '/functions.php';
