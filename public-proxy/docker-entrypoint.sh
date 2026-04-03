#!/bin/bash
set -e

# SupplyCore Public Proxy — Docker Entrypoint
#
# Generates config.php from environment variables so the proxy
# is ready to serve immediately without manual setup.

CONFIG_FILE="/var/www/html/config.php"

if [ -n "$SUPPLYCORE_URL" ] && [ -n "$API_KEY_ID" ] && [ -n "$API_SECRET" ]; then
    php -r '
        $config = [
            "supplycore_url" => getenv("SUPPLYCORE_URL"),
            "api_key_id"     => getenv("API_KEY_ID"),
            "api_secret"     => getenv("API_SECRET"),
            "timeout"        => (int) (getenv("TIMEOUT") ?: 15),
            "site_name"      => getenv("SITE_NAME") ?: "Battle Reports",
        ];
        $export = var_export($config, true);
        file_put_contents(
            "/var/www/html/config.php",
            "<?php\n\nreturn {$export};\n"
        );
    '
    echo "[entrypoint] config.php generated from environment variables."
elif [ -f "$CONFIG_FILE" ]; then
    echo "[entrypoint] Using existing config.php."
elif [ -n "$PROXY_CONFIG_KEY" ] && [ -f "/var/www/html/.config.vault" ]; then
    echo "[entrypoint] Using encrypted vault with PROXY_CONFIG_KEY."
else
    echo "[entrypoint] WARNING: No configuration found."
    echo "[entrypoint] Set SUPPLYCORE_URL, API_KEY_ID, and API_SECRET environment variables."
fi

exec "$@"
