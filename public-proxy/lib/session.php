<?php

declare(strict_types=1);

/**
 * SupplyCore Public Proxy — Session & IP-hash storage (SQLite).
 *
 * This module owns *all* persistent state for the proxy:
 *   - Logged-in user sessions (via ESI SSO)
 *   - Per-character IP-hash log for cross-matching
 *   - OAuth state tokens for CSRF protection on the SSO flow
 *
 * All data lives in a single SQLite database so the proxy remains
 * completely self-contained (no MariaDB required on the proxy host).
 *
 * Privacy model — IMPORTANT:
 *   We NEVER store raw IP addresses. Every IP is hashed with:
 *       SHA-256(ip_address || pepper)
 *   The pepper is loaded from the "ip_hash_salt" config key (64-byte secret).
 *   Cross-matching still works because the same IP + pepper produces the
 *   same hash, but the raw address cannot be recovered from the database.
 *
 *   Storing and cross-matching network identifiers may still be regulated
 *   as personal data under GDPR, CCPA, and similar laws even after hashing.
 *   Always show the privacy disclosure before a user logs in and honor
 *   deletion requests. See public-proxy/privacy.php for the notice.
 */

require_once __DIR__ . '/api-client.php';

const PROXY_SESSION_COOKIE      = 'sc_session';
const PROXY_SESSION_TTL_SECONDS = 2592000; // 30 days
const PROXY_OAUTH_STATE_TTL     = 600;     // 10 minutes

/**
 * Return the PDO handle to the proxy's SQLite database, creating
 * the schema on first access.
 */
function proxy_session_db(): PDO
{
    static $pdo = null;
    if ($pdo instanceof PDO) {
        return $pdo;
    }

    $config = proxy_config();
    $path = (string) ($config['session_db_path'] ?? (__DIR__ . '/../storage/session.sqlite'));

    $dir = dirname($path);
    if (!is_dir($dir)) {
        if (!@mkdir($dir, 0o750, true) && !is_dir($dir)) {
            throw new RuntimeException('Cannot create session storage directory: ' . $dir);
        }
    }

    $pdo = new PDO('sqlite:' . $path, null, null, [
        PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ]);
    $pdo->exec('PRAGMA journal_mode = WAL');
    $pdo->exec('PRAGMA synchronous = NORMAL');
    $pdo->exec('PRAGMA foreign_keys = ON');

    proxy_session_schema_init($pdo);
    return $pdo;
}

function proxy_session_schema_init(PDO $pdo): void
{
    $pdo->exec('CREATE TABLE IF NOT EXISTS sessions (
        session_id       TEXT PRIMARY KEY,
        character_id     INTEGER NOT NULL,
        character_name   TEXT    NOT NULL,
        corporation_id   INTEGER,
        corporation_name TEXT,
        alliance_id      INTEGER,
        alliance_name    TEXT,
        ip_hash          TEXT,
        user_agent_hash  TEXT,
        created_at       INTEGER NOT NULL,
        last_seen_at     INTEGER NOT NULL,
        expires_at       INTEGER NOT NULL
    )');
    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_sessions_char ON sessions(character_id)');
    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at)');

    $pdo->exec('CREATE TABLE IF NOT EXISTS character_profiles (
        character_id     INTEGER PRIMARY KEY,
        character_name   TEXT    NOT NULL,
        corporation_id   INTEGER,
        corporation_name TEXT,
        alliance_id      INTEGER,
        alliance_name    TEXT,
        first_seen_at    INTEGER NOT NULL,
        last_seen_at     INTEGER NOT NULL,
        login_count      INTEGER NOT NULL DEFAULT 0
    )');

    $pdo->exec('CREATE TABLE IF NOT EXISTS character_ip_log (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        character_id     INTEGER NOT NULL,
        ip_hash          TEXT    NOT NULL,
        user_agent_hash  TEXT,
        first_seen_at    INTEGER NOT NULL,
        last_seen_at     INTEGER NOT NULL,
        hit_count        INTEGER NOT NULL DEFAULT 1,
        UNIQUE(character_id, ip_hash)
    )');
    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_ip_hash ON character_ip_log(ip_hash)');
    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_ip_char ON character_ip_log(character_id)');

    $pdo->exec('CREATE TABLE IF NOT EXISTS oauth_state (
        state      TEXT PRIMARY KEY,
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        return_to  TEXT
    )');
    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_oauth_state_expires ON oauth_state(expires_at)');
}

/**
 * Determine the client IP address. Respects X-Forwarded-For if the
 * proxy is behind a trusted reverse-proxy (set trust_forwarded_for
 * in config to enable).
 */
function proxy_client_ip(): string
{
    $config = proxy_config();
    $trustForwarded = (bool) ($config['trust_forwarded_for'] ?? false);

    if ($trustForwarded && !empty($_SERVER['HTTP_X_FORWARDED_FOR'])) {
        $parts = explode(',', (string) $_SERVER['HTTP_X_FORWARDED_FOR']);
        $first = trim((string) $parts[0]);
        if ($first !== '') {
            return $first;
        }
    }

    return (string) ($_SERVER['REMOTE_ADDR'] ?? '');
}

/**
 * Hash an IP address with the configured pepper. Returns a 64-char
 * lowercase hex string, or an empty string if the input is empty.
 */
function proxy_hash_ip(string $ip): string
{
    if ($ip === '') {
        return '';
    }
    $config = proxy_config();
    $pepper = (string) ($config['ip_hash_salt'] ?? '');
    if ($pepper === '') {
        // Refuse to hash without a pepper — rainbow-table protection is the whole point.
        throw new RuntimeException('Missing ip_hash_salt in proxy config. Generate one with: php -r "echo bin2hex(random_bytes(32));"');
    }
    return hash('sha256', $ip . '|' . $pepper);
}

function proxy_hash_user_agent(string $ua): string
{
    if ($ua === '') {
        return '';
    }
    // Short peppered hash — enough to cluster, not enough to leak raw UA.
    $config = proxy_config();
    $pepper = (string) ($config['ip_hash_salt'] ?? '');
    return substr(hash('sha256', $ua . '|' . $pepper), 0, 32);
}

/**
 * Record that a character was seen from a (hashed) IP. Updates hit
 * count and last_seen_at on subsequent visits.
 */
function proxy_record_character_ip(int $characterId, string $ipHash, string $uaHash): void
{
    if ($characterId <= 0 || $ipHash === '') {
        return;
    }
    $pdo = proxy_session_db();
    $now = time();

    $stmt = $pdo->prepare('SELECT id, hit_count FROM character_ip_log WHERE character_id = ? AND ip_hash = ? LIMIT 1');
    $stmt->execute([$characterId, $ipHash]);
    $existing = $stmt->fetch();

    if ($existing) {
        $upd = $pdo->prepare('UPDATE character_ip_log SET hit_count = hit_count + 1, last_seen_at = ?, user_agent_hash = COALESCE(?, user_agent_hash) WHERE id = ?');
        $upd->execute([$now, $uaHash !== '' ? $uaHash : null, (int) $existing['id']]);
        return;
    }

    $ins = $pdo->prepare('INSERT INTO character_ip_log (character_id, ip_hash, user_agent_hash, first_seen_at, last_seen_at, hit_count) VALUES (?, ?, ?, ?, ?, 1)');
    $ins->execute([$characterId, $ipHash, $uaHash !== '' ? $uaHash : null, $now, $now]);
}

/**
 * Upsert a character profile on login.
 */
function proxy_upsert_character_profile(array $profile): void
{
    $pdo = proxy_session_db();
    $now = time();

    $stmt = $pdo->prepare('SELECT character_id FROM character_profiles WHERE character_id = ? LIMIT 1');
    $stmt->execute([(int) $profile['character_id']]);
    $exists = (bool) $stmt->fetchColumn();

    if ($exists) {
        $upd = $pdo->prepare('UPDATE character_profiles
            SET character_name = ?, corporation_id = ?, corporation_name = ?,
                alliance_id = ?, alliance_name = ?, last_seen_at = ?, login_count = login_count + 1
            WHERE character_id = ?');
        $upd->execute([
            (string) $profile['character_name'],
            (int) ($profile['corporation_id'] ?? 0) ?: null,
            (string) ($profile['corporation_name'] ?? '') ?: null,
            (int) ($profile['alliance_id'] ?? 0) ?: null,
            (string) ($profile['alliance_name'] ?? '') ?: null,
            $now,
            (int) $profile['character_id'],
        ]);
        return;
    }

    $ins = $pdo->prepare('INSERT INTO character_profiles
        (character_id, character_name, corporation_id, corporation_name, alliance_id, alliance_name, first_seen_at, last_seen_at, login_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)');
    $ins->execute([
        (int) $profile['character_id'],
        (string) $profile['character_name'],
        (int) ($profile['corporation_id'] ?? 0) ?: null,
        (string) ($profile['corporation_name'] ?? '') ?: null,
        (int) ($profile['alliance_id'] ?? 0) ?: null,
        (string) ($profile['alliance_name'] ?? '') ?: null,
        $now,
        $now,
    ]);
}

/**
 * Create a new session row and set the session cookie.
 */
function proxy_session_create(array $profile): string
{
    $pdo = proxy_session_db();

    $sessionId = bin2hex(random_bytes(32));
    $now = time();
    $expires = $now + PROXY_SESSION_TTL_SECONDS;

    $ipHash = proxy_hash_ip(proxy_client_ip());
    $uaHash = proxy_hash_user_agent((string) ($_SERVER['HTTP_USER_AGENT'] ?? ''));

    $stmt = $pdo->prepare('INSERT INTO sessions
        (session_id, character_id, character_name, corporation_id, corporation_name, alliance_id, alliance_name, ip_hash, user_agent_hash, created_at, last_seen_at, expires_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
    $stmt->execute([
        $sessionId,
        (int) $profile['character_id'],
        (string) $profile['character_name'],
        (int) ($profile['corporation_id'] ?? 0) ?: null,
        (string) ($profile['corporation_name'] ?? '') ?: null,
        (int) ($profile['alliance_id'] ?? 0) ?: null,
        (string) ($profile['alliance_name'] ?? '') ?: null,
        $ipHash,
        $uaHash,
        $now,
        $now,
        $expires,
    ]);

    proxy_upsert_character_profile($profile);
    proxy_record_character_ip((int) $profile['character_id'], $ipHash, $uaHash);

    $secure = (($_SERVER['HTTPS'] ?? '') !== '') || (($_SERVER['HTTP_X_FORWARDED_PROTO'] ?? '') === 'https');
    setcookie(PROXY_SESSION_COOKIE, $sessionId, [
        'expires'  => $expires,
        'path'     => '/',
        'secure'   => $secure,
        'httponly' => true,
        'samesite' => 'Lax',
    ]);

    return $sessionId;
}

/**
 * Fetch the current session (if any) from the cookie. Updates
 * last_seen_at and records the hashed IP on each request.
 */
function proxy_session_current(): ?array
{
    static $cached = false;
    static $session = null;
    if ($cached) {
        return $session;
    }
    $cached = true;

    $sessionId = (string) ($_COOKIE[PROXY_SESSION_COOKIE] ?? '');
    if ($sessionId === '' || !ctype_xdigit($sessionId)) {
        return $session;
    }

    try {
        $pdo = proxy_session_db();
    } catch (Throwable $e) {
        error_log('[proxy-session] db open failed: ' . $e->getMessage());
        return $session;
    }

    $stmt = $pdo->prepare('SELECT * FROM sessions WHERE session_id = ? LIMIT 1');
    $stmt->execute([$sessionId]);
    $row = $stmt->fetch();

    if (!$row) {
        return $session;
    }

    $now = time();
    if ((int) $row['expires_at'] < $now) {
        $pdo->prepare('DELETE FROM sessions WHERE session_id = ?')->execute([$sessionId]);
        return $session;
    }

    // Opportunistic tracking refresh (once per minute per session).
    if ((int) $row['last_seen_at'] < $now - 60) {
        $ipHash = proxy_hash_ip(proxy_client_ip());
        $uaHash = proxy_hash_user_agent((string) ($_SERVER['HTTP_USER_AGENT'] ?? ''));
        $pdo->prepare('UPDATE sessions SET last_seen_at = ?, ip_hash = ? WHERE session_id = ?')
            ->execute([$now, $ipHash, $sessionId]);
        proxy_record_character_ip((int) $row['character_id'], $ipHash, $uaHash);
    }

    $session = $row;
    return $session;
}

function proxy_session_destroy(): void
{
    $sessionId = (string) ($_COOKIE[PROXY_SESSION_COOKIE] ?? '');
    if ($sessionId !== '') {
        try {
            proxy_session_db()->prepare('DELETE FROM sessions WHERE session_id = ?')->execute([$sessionId]);
        } catch (Throwable $e) {
            // ignore — cookie will be cleared anyway
        }
    }
    setcookie(PROXY_SESSION_COOKIE, '', [
        'expires'  => time() - 3600,
        'path'     => '/',
        'httponly' => true,
        'samesite' => 'Lax',
    ]);
}

// ── OAuth CSRF state ─────────────────────────────────────────────────

function proxy_oauth_state_create(?string $returnTo = null): string
{
    $pdo = proxy_session_db();
    $state = bin2hex(random_bytes(24));
    $now = time();
    $pdo->prepare('INSERT INTO oauth_state (state, created_at, expires_at, return_to) VALUES (?, ?, ?, ?)')
        ->execute([$state, $now, $now + PROXY_OAUTH_STATE_TTL, $returnTo]);

    // Opportunistic cleanup
    $pdo->prepare('DELETE FROM oauth_state WHERE expires_at < ?')->execute([$now]);
    return $state;
}

function proxy_oauth_state_consume(string $state): ?array
{
    if ($state === '') {
        return null;
    }
    $pdo = proxy_session_db();

    $stmt = $pdo->prepare('SELECT state, expires_at, return_to FROM oauth_state WHERE state = ? LIMIT 1');
    $stmt->execute([$state]);
    $row = $stmt->fetch();

    if (!$row) {
        return null;
    }
    $pdo->prepare('DELETE FROM oauth_state WHERE state = ?')->execute([$state]);

    if ((int) $row['expires_at'] < time()) {
        return null;
    }
    return $row;
}

// ── Cross-match lookup (admin tooling) ────────────────────────────────

/**
 * Given a character_id, return the list of *other* character_ids that
 * have been observed from at least one shared (hashed) IP. Returns an
 * array of [character_id, character_name, shared_ips, total_hits].
 */
function proxy_crossmatch_for_character(int $characterId, int $limit = 25): array
{
    if ($characterId <= 0) {
        return [];
    }
    $pdo = proxy_session_db();

    $sql = 'SELECT cp.character_id, cp.character_name, cp.corporation_name, cp.alliance_name,
                   COUNT(DISTINCT l2.ip_hash) AS shared_ips,
                   SUM(l2.hit_count) AS total_hits
            FROM character_ip_log l1
            INNER JOIN character_ip_log l2
                ON l2.ip_hash = l1.ip_hash
                AND l2.character_id <> l1.character_id
            LEFT JOIN character_profiles cp
                ON cp.character_id = l2.character_id
            WHERE l1.character_id = ?
            GROUP BY cp.character_id, cp.character_name, cp.corporation_name, cp.alliance_name
            ORDER BY shared_ips DESC, total_hits DESC
            LIMIT ' . max(1, min(200, $limit));

    $stmt = $pdo->prepare($sql);
    $stmt->execute([$characterId]);
    return $stmt->fetchAll() ?: [];
}
