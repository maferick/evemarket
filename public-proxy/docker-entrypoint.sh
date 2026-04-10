#!/bin/bash
set -e

# SupplyCore Public Proxy — Docker Entrypoint
#
# Generates config.php from environment variables so the proxy
# is ready to serve immediately without manual setup.
#
# Supported environment variables:
#   SUPPLYCORE_URL       Base URL of SupplyCore (required)
#   API_KEY_ID           Public API key ID (required)
#   API_SECRET           Public API shared secret (required)
#   TIMEOUT              HTTP timeout in seconds (default 15)
#   SITE_NAME            Branding shown in nav bar (default "Battle Reports")
#
#   ESI_CLIENT_ID        EVE SSO application client id (optional)
#   ESI_CLIENT_SECRET    EVE SSO application client secret (optional)
#   ESI_CALLBACK_URL     Full URL pointing to /callback on this proxy
#   IP_HASH_SALT         Hex pepper for IP hashes (required if ESI enabled)
#   TRUST_FORWARDED_FOR  "1" to honor X-Forwarded-For from a reverse proxy
#   PRIVACY_CONTACT      Contact line on the privacy page
#   SESSION_DB_PATH      Override SQLite path (default /var/www/html/storage/session.sqlite)

CONFIG_FILE="/var/www/html/config.php"
STORAGE_DIR="/var/www/html/storage"

# Ensure storage dir exists and is writable by the PHP process
mkdir -p "$STORAGE_DIR"
chown -R www-data:www-data "$STORAGE_DIR" 2>/dev/null || true
chmod 0750 "$STORAGE_DIR" 2>/dev/null || true

if [ -n "$SUPPLYCORE_URL" ] && [ -n "$API_KEY_ID" ] && [ -n "$API_SECRET" ]; then
    php -r '
        $config = [
            "supplycore_url"       => getenv("SUPPLYCORE_URL"),
            "api_key_id"           => getenv("API_KEY_ID"),
            "api_secret"           => getenv("API_SECRET"),
            "timeout"              => (int) (getenv("TIMEOUT") ?: 15),
            "site_name"            => getenv("SITE_NAME") ?: "Battle Reports",
            "esi_client_id"        => getenv("ESI_CLIENT_ID")     ?: "",
            "esi_client_secret"    => getenv("ESI_CLIENT_SECRET") ?: "",
            "esi_callback_url"     => getenv("ESI_CALLBACK_URL")  ?: "",
            "ip_hash_salt"         => getenv("IP_HASH_SALT")      ?: "",
            "session_db_path"      => getenv("SESSION_DB_PATH")   ?: "/var/www/html/storage/session.sqlite",
            "trust_forwarded_for"  => (bool) (getenv("TRUST_FORWARDED_FOR") ?: false),
            "privacy_contact"      => getenv("PRIVACY_CONTACT") ?: "",
        ];
        $export = var_export($config, true);
        file_put_contents(
            "/var/www/html/config.php",
            "<?php\n\nreturn {$export};\n"
        );
    '
    echo "[entrypoint] config.php generated from environment variables."

    # Warn if ESI is half-configured.
    if [ -n "$ESI_CLIENT_ID" ] || [ -n "$ESI_CLIENT_SECRET" ] || [ -n "$ESI_CALLBACK_URL" ]; then
        if [ -z "$ESI_CLIENT_ID" ] || [ -z "$ESI_CLIENT_SECRET" ] || [ -z "$ESI_CALLBACK_URL" ] || [ -z "$IP_HASH_SALT" ]; then
            echo "[entrypoint] WARNING: ESI login is partially configured."
            echo "[entrypoint]   ESI_CLIENT_ID:     $( [ -n "$ESI_CLIENT_ID" ] && echo set || echo MISSING)"
            echo "[entrypoint]   ESI_CLIENT_SECRET: $( [ -n "$ESI_CLIENT_SECRET" ] && echo set || echo MISSING)"
            echo "[entrypoint]   ESI_CALLBACK_URL:  $( [ -n "$ESI_CALLBACK_URL" ] && echo set || echo MISSING)"
            echo "[entrypoint]   IP_HASH_SALT:      $( [ -n "$IP_HASH_SALT" ] && echo set || echo MISSING)"
            echo "[entrypoint] Login flow will fail until all four are provided."
        fi
    fi
elif [ -f "$CONFIG_FILE" ]; then
    echo "[entrypoint] Using existing config.php."
elif [ -n "$PROXY_CONFIG_KEY" ] && [ -f "/var/www/html/.config.vault" ]; then
    echo "[entrypoint] Using encrypted vault with PROXY_CONFIG_KEY."
else
    echo "[entrypoint] WARNING: No configuration found."
    echo "[entrypoint] Set SUPPLYCORE_URL, API_KEY_ID, and API_SECRET environment variables."
fi

exec "$@"
