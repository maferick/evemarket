#!/usr/bin/env php
<?php

declare(strict_types=1);

/**
 * SupplyCore Public Proxy — Interactive Configuration Generator
 *
 * Prompts for proxy settings and writes an encrypted .config.vault blob.
 *
 * Usage:
 *   php bin/configure.php              # Interactive mode
 *   php bin/configure.php --show       # Decrypt and display current config
 *   php bin/configure.php --rotate     # Re-encrypt with a new passphrase
 *
 * The passphrase is read from PROXY_CONFIG_KEY env var, or prompted if unset.
 */

if (php_sapi_name() !== 'cli') {
    echo 'This script must be run from the command line.';
    exit(1);
}

require __DIR__ . '/../lib/config-vault.php';

// ─── Helpers ─────────────────────────────────────────────────────────

function out(string $msg): void { fwrite(STDOUT, $msg); }
function err(string $msg): void { fwrite(STDERR, "\033[31m✗ {$msg}\033[0m\n"); }
function ok(string $msg): void  { fwrite(STDOUT, "\033[32m✓ {$msg}\033[0m\n"); }
function info(string $msg): void { fwrite(STDOUT, "\033[36m  {$msg}\033[0m\n"); }

function prompt(string $label, string $default = ''): string
{
    $hint = $default !== '' ? " [{$default}]" : '';
    out("{$label}{$hint}: ");
    $value = trim((string) fgets(STDIN));
    return $value !== '' ? $value : $default;
}

function prompt_secret(string $label): string
{
    // Try to hide input on terminals that support it
    $stty = @shell_exec('stty -echo 2>/dev/null');
    out("{$label}: ");
    $value = trim((string) fgets(STDIN));
    if ($stty !== null) {
        @shell_exec('stty echo');
        out("\n");
    }
    return $value;
}

function get_passphrase(string $context = 'Vault passphrase'): string
{
    $key = getenv('PROXY_CONFIG_KEY');
    if ($key !== false && $key !== '') {
        info("Using passphrase from PROXY_CONFIG_KEY environment variable.");
        return $key;
    }
    $pass = prompt_secret($context);
    if ($pass === '') {
        err("Passphrase cannot be empty.");
        exit(1);
    }
    return $pass;
}

// ─── Commands ────────────────────────────────────────────────────────

$vaultFile = __DIR__ . '/../.config.vault';
$command   = $argv[1] ?? '';

// --- Provision from SupplyCore ---
if ($command === '--provision') {
    $provisionUrl = $argv[2] ?? '';
    if ($provisionUrl === '') {
        err("Usage: php bin/configure.php --provision <provision-url>");
        info("The provision URL is provided by SupplyCore when you generate a provisioning token.");
        info("Example: php bin/configure.php --provision 'https://supplycore.example.com/api/public/provision.php?token=abc123'");
        exit(1);
    }

    out("\n\033[1m╔══════════════════════════════════════════════════════╗\033[0m\n");
    out("\033[1m║   SupplyCore Public Proxy — Remote Provisioning      ║\033[0m\n");
    out("\033[1m╚══════════════════════════════════════════════════════╝\033[0m\n\n");

    info("Fetching configuration from SupplyCore...");

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL            => $provisionUrl,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT        => 15,
        CURLOPT_CONNECTTIMEOUT => 5,
        CURLOPT_FOLLOWLOCATION => false,
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_HTTPHEADER     => ['Accept: application/json'],
    ]);

    $response  = curl_exec($ch);
    $httpCode  = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($response === false || $curlError !== '') {
        err("Connection failed: {$curlError}");
        exit(1);
    }

    if ($httpCode !== 200) {
        $decoded = json_decode((string) $response, true);
        $msg = is_array($decoded) ? (string) ($decoded['error'] ?? "HTTP {$httpCode}") : "HTTP {$httpCode}";
        err("Provisioning failed: {$msg}");
        exit(1);
    }

    $decoded = json_decode((string) $response, true);
    if (!is_array($decoded) || !is_array($decoded['config'] ?? null)) {
        err("Invalid response from SupplyCore.");
        exit(1);
    }

    $config = $decoded['config'];
    ok("Received configuration from SupplyCore.");
    info("Endpoint:   " . ($config['supplycore_url'] ?? '(unknown)'));
    info("API Key:    " . ($config['api_key_id'] ?? '(unknown)'));
    info("Site Name:  " . ($config['site_name'] ?? '(unknown)'));

    // Auto-generate a strong passphrase
    $passphrase = bin2hex(random_bytes(24));

    $blob = vault_encrypt($config, $passphrase);
    file_put_contents($vaultFile, $blob);
    chmod($vaultFile, 0600);

    out("\n");
    ok("Configuration encrypted and saved.");
    info("Vault file: {$vaultFile}");
    info("Blob size:  " . strlen($blob) . " bytes");

    out("\n\033[1m  Set this environment variable on the proxy server:\033[0m\n\n");
    out("  \033[33mexport PROXY_CONFIG_KEY='{$passphrase}'\033[0m\n\n");
    info("Add it to your systemd unit, Apache envvars, or .bashrc.");
    info("The provisioning token has been consumed and cannot be reused.");
    out("\n");
    exit(0);
}

// --- Show current config ---
if ($command === '--show') {
    if (!file_exists($vaultFile)) {
        err("No vault file found. Run 'php bin/configure.php' to create one.");
        exit(1);
    }

    $passphrase = get_passphrase('Decryption passphrase');

    try {
        $blob   = file_get_contents($vaultFile);
        $config = vault_decrypt($blob, $passphrase);
    } catch (RuntimeException $e) {
        err($e->getMessage());
        exit(1);
    }

    out("\n\033[1mCurrent vault configuration:\033[0m\n\n");
    $masked = $config;
    if (isset($masked['api_secret'])) {
        $s = $masked['api_secret'];
        $masked['api_secret'] = substr($s, 0, 8) . str_repeat('•', max(0, strlen($s) - 12)) . substr($s, -4);
    }
    foreach ($masked as $key => $value) {
        out(sprintf("  \033[33m%-18s\033[0m %s\n", $key, (string) $value));
    }
    out("\n");
    info("Vault file: {$vaultFile}");
    info("Blob size:  " . number_format(strlen($blob)) . " bytes");
    exit(0);
}

// --- Rotate passphrase ---
if ($command === '--rotate') {
    if (!file_exists($vaultFile)) {
        err("No vault file found. Nothing to rotate.");
        exit(1);
    }

    out("Enter CURRENT passphrase to decrypt:\n");
    $oldPass = get_passphrase('Current passphrase');

    try {
        $blob   = file_get_contents($vaultFile);
        $config = vault_decrypt($blob, $oldPass);
    } catch (RuntimeException $e) {
        err($e->getMessage());
        exit(1);
    }

    ok("Decrypted successfully.");
    out("\nEnter NEW passphrase to re-encrypt:\n");
    $newPass = prompt_secret('New passphrase');
    if ($newPass === '') {
        err("New passphrase cannot be empty.");
        exit(1);
    }
    $confirm = prompt_secret('Confirm new passphrase');
    if ($newPass !== $confirm) {
        err("Passphrases do not match.");
        exit(1);
    }

    $newBlob = vault_encrypt($config, $newPass);
    file_put_contents($vaultFile, $newBlob);
    chmod($vaultFile, 0600);

    ok("Vault re-encrypted with new passphrase.");
    info("Remember to update the PROXY_CONFIG_KEY environment variable.");
    exit(0);
}

// --- Interactive setup ---
out("\n\033[1m╔══════════════════════════════════════════════════════╗\033[0m\n");
out("\033[1m║   SupplyCore Public Proxy — Configuration Setup      ║\033[0m\n");
out("\033[1m╚══════════════════════════════════════════════════════╝\033[0m\n\n");

// Load existing config as defaults if vault exists
$defaults = [
    'supplycore_url' => 'https://supplycore.example.com',
    'api_key_id'     => '',
    'api_secret'     => '',
    'timeout'        => '15',
    'site_name'      => 'Battle Reports',
];

if (file_exists($vaultFile)) {
    info("Existing vault found — values will be used as defaults.\n");
    $existPass = get_passphrase('Passphrase to load existing vault');
    try {
        $blob     = file_get_contents($vaultFile);
        $existing = vault_decrypt($blob, $existPass);
        $defaults = array_merge($defaults, array_map('strval', $existing));
        ok("Loaded existing configuration.\n");
    } catch (RuntimeException $e) {
        err("Could not decrypt existing vault: " . $e->getMessage());
        out("  Continuing with empty defaults.\n\n");
    }
}

out("\033[1mSupplyCore Connection\033[0m\n");
out("─────────────────────\n");
$url     = prompt('  SupplyCore base URL (no trailing slash)', $defaults['supplycore_url']);
$keyId   = prompt('  API key ID (starts with pk_)', $defaults['api_key_id']);
$secret  = prompt('  API shared secret (64-char hex)', $defaults['api_secret']);
$timeout = prompt('  Request timeout in seconds', $defaults['timeout']);

out("\n\033[1mProxy Branding\033[0m\n");
out("──────────────\n");
$siteName = prompt('  Site name shown in header', $defaults['site_name']);

// Validate
$errors = [];
if (!filter_var($url, FILTER_VALIDATE_URL)) {
    $errors[] = "Invalid URL: {$url}";
}
if (!str_starts_with($keyId, 'pk_')) {
    $errors[] = "API key ID should start with 'pk_' (got: {$keyId})";
}
if (strlen($secret) < 32) {
    $errors[] = "API secret looks too short (expected 64-char hex, got " . strlen($secret) . " chars)";
}

if ($errors) {
    out("\n\033[33mWarnings:\033[0m\n");
    foreach ($errors as $e) {
        out("  ⚠ {$e}\n");
    }
    $proceed = prompt("\nContinue anyway? (y/N)", 'N');
    if (strtolower($proceed) !== 'y') {
        err("Aborted.");
        exit(1);
    }
}

$config = [
    'supplycore_url' => $url,
    'api_key_id'     => $keyId,
    'api_secret'     => $secret,
    'timeout'        => (int) $timeout,
    'site_name'      => $siteName,
];

// Get passphrase for encryption
out("\n\033[1mEncryption\033[0m\n");
out("──────────\n");
$passphrase = get_passphrase('  Vault passphrase (or set PROXY_CONFIG_KEY env var)');

if (getenv('PROXY_CONFIG_KEY') === false || getenv('PROXY_CONFIG_KEY') === '') {
    $confirm = prompt_secret('  Confirm passphrase');
    if ($passphrase !== $confirm) {
        err("Passphrases do not match.");
        exit(1);
    }
}

// Encrypt and write
$blob = vault_encrypt($config, $passphrase);
file_put_contents($vaultFile, $blob);
chmod($vaultFile, 0600);

out("\n");
ok("Configuration encrypted and saved.");
info("Vault file: {$vaultFile}");
info("Blob size:  " . strlen($blob) . " bytes");
info("File perms: 0600 (owner read/write only)");
out("\n");
info("To use this vault, set the passphrase as an environment variable:");
out("  \033[33mexport PROXY_CONFIG_KEY='your-passphrase'\033[0m\n\n");
info("You can remove config.php if it exists — the vault takes priority.");
out("\n");
