<?php

declare(strict_types=1);

/**
 * SupplyCore Public Proxy — Encrypted Configuration Vault
 *
 * Stores proxy configuration as an encrypted binary blob that cannot be
 * read without the passphrase. Uses AES-256-CBC for encryption with
 * HMAC-SHA256 for tamper detection (encrypt-then-MAC).
 *
 * Key derivation: PBKDF2-SHA256 (100 000 iterations) from passphrase + salt
 *   → 32 bytes encryption key + 32 bytes HMAC key
 *
 * Blob format (binary, big-endian):
 *   [4]  magic   "SCV1"
 *   [32] salt    PBKDF2 salt
 *   [16] iv      AES-CBC initialization vector
 *   [32] mac     HMAC-SHA256 over (salt ‖ iv ‖ ciphertext)
 *   [N]  ct      AES-256-CBC ciphertext (PKCS7 padded)
 *
 * The passphrase is read from the PROXY_CONFIG_KEY environment variable.
 */

const VAULT_MAGIC      = "SCV1";
const VAULT_SALT_LEN   = 32;
const VAULT_IV_LEN     = 16;   // AES block size
const VAULT_MAC_LEN    = 32;   // SHA-256 output
const VAULT_KDF_ITER   = 100_000;
const VAULT_CIPHER     = 'aes-256-cbc';
const VAULT_HEADER_LEN = 4 + VAULT_SALT_LEN + VAULT_IV_LEN + VAULT_MAC_LEN; // 84 bytes

/**
 * Default vault file path.
 */
function vault_path(): string
{
    return __DIR__ . '/../.config.vault';
}

/**
 * Derive a 64-byte key (32 enc + 32 mac) from passphrase and salt.
 */
function vault_derive_keys(string $passphrase, string $salt): array
{
    $derived = hash_pbkdf2('sha256', $passphrase, $salt, VAULT_KDF_ITER, 64, true);
    return [
        'enc' => substr($derived, 0, 32),
        'mac' => substr($derived, 32, 32),
    ];
}

/**
 * Encrypt a configuration array into a vault blob.
 *
 * @param  array  $config     Proxy configuration key-value pairs
 * @param  string $passphrase Encryption passphrase
 * @return string Binary vault blob
 */
function vault_encrypt(array $config, string $passphrase): string
{
    $plaintext = json_encode($config, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);

    $salt = random_bytes(VAULT_SALT_LEN);
    $iv   = random_bytes(VAULT_IV_LEN);
    $keys = vault_derive_keys($passphrase, $salt);

    $ct = openssl_encrypt($plaintext, VAULT_CIPHER, $keys['enc'], OPENSSL_RAW_DATA, $iv);
    if ($ct === false) {
        throw new RuntimeException('Encryption failed: ' . openssl_error_string());
    }

    // Encrypt-then-MAC: HMAC covers salt, IV, and ciphertext
    $mac = hash_hmac('sha256', $salt . $iv . $ct, $keys['mac'], true);

    return VAULT_MAGIC . $salt . $iv . $mac . $ct;
}

/**
 * Decrypt a vault blob back into a configuration array.
 *
 * @param  string $blob       Raw vault file contents
 * @param  string $passphrase Decryption passphrase
 * @return array  Proxy configuration
 *
 * @throws RuntimeException On bad magic, HMAC mismatch, or decryption failure
 */
function vault_decrypt(string $blob, string $passphrase): array
{
    if (strlen($blob) < VAULT_HEADER_LEN + 16) {
        throw new RuntimeException('Vault file is too small or corrupted.');
    }

    $magic = substr($blob, 0, 4);
    if ($magic !== VAULT_MAGIC) {
        throw new RuntimeException('Not a valid vault file (bad magic).');
    }

    $salt = substr($blob, 4, VAULT_SALT_LEN);
    $iv   = substr($blob, 4 + VAULT_SALT_LEN, VAULT_IV_LEN);
    $mac  = substr($blob, 4 + VAULT_SALT_LEN + VAULT_IV_LEN, VAULT_MAC_LEN);
    $ct   = substr($blob, VAULT_HEADER_LEN);

    $keys = vault_derive_keys($passphrase, $salt);

    // Verify HMAC before attempting decryption (reject tampering early)
    $expected = hash_hmac('sha256', $salt . $iv . $ct, $keys['mac'], true);
    if (!hash_equals($expected, $mac)) {
        throw new RuntimeException('HMAC verification failed — wrong passphrase or tampered vault.');
    }

    $plaintext = openssl_decrypt($ct, VAULT_CIPHER, $keys['enc'], OPENSSL_RAW_DATA, $iv);
    if ($plaintext === false) {
        throw new RuntimeException('Decryption failed: ' . openssl_error_string());
    }

    $config = json_decode($plaintext, true, 8, JSON_THROW_ON_ERROR);
    if (!is_array($config)) {
        throw new RuntimeException('Vault contains invalid configuration data.');
    }

    return $config;
}

/**
 * Load configuration from the encrypted vault.
 *
 * Reads the passphrase from the PROXY_CONFIG_KEY environment variable.
 *
 * @param  string|null $path Override vault file path
 * @return array|null  Config array, or null if vault unavailable
 */
function vault_load(?string $path = null): ?array
{
    $path = $path ?? vault_path();

    if (!file_exists($path)) {
        return null;
    }

    $passphrase = getenv('PROXY_CONFIG_KEY');
    if ($passphrase === false || $passphrase === '') {
        return null;
    }

    $blob = file_get_contents($path);
    if ($blob === false || $blob === '') {
        return null;
    }

    return vault_decrypt($blob, $passphrase);
}
