# EveMarket Foundation

A production-oriented baseline for **EveMarket** built with **PHP 8+, MySQL, Apache2**, and a modern **Tailwind CSS v4 + shadcn/ui-inspired** interface.

This repository establishes a clean architecture that can scale from an initial dashboard/settings app into a complete market, trading, and ESI-integrated platform.

## Features Included

- Product-style dashboard homepage with operational cards and quick actions
- Expandable navigation architecture with parent and nested submenu items
- Modular settings system (section-based, not flat)
  - General settings
  - Trading Stations (market + alliance station selection)
  - ESI Login settings
  - Data Sync settings with incremental SQL update toggle
- `src/db.php` as the **single central database access layer**
- `src/functions.php` as the **single shared helper/business utility layer**
- Secure CSRF-protected settings forms
- Session-based flash messaging

## Project Structure

```text
public/
  index.php                 # Dashboard
  settings/index.php        # Modular settings controller/view
  .htaccess                 # Apache rewrite rules
src/
  bootstrap.php             # Session + shared bootstrap
  db.php                    # Config + PDO + query helpers + transactions
  functions.php             # Navigation, settings services, shared helpers
  config/app.php            # Environment-driven app/db config
  views/partials/           # Header / sidebar / footer layout partials
database/
  schema.sql                # Tables + starter data
AGENTS.md                   # Instructions for coding agents
README.md
```

## Quick Start (Local)

1. **Create database and import schema**
   ```bash
   mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS evemarket CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
   mysql -u root -p evemarket < database/schema.sql
   ```

2. **Set environment variables** (example)
   ```bash
   export APP_ENV=development
   export APP_BASE_URL=http://localhost:8080
   export DB_HOST=127.0.0.1
   export DB_PORT=3306
   export DB_DATABASE=evemarket
   export DB_USERNAME=root
   export DB_PASSWORD=secret
   ```

3. **Run with PHP built-in server (dev only)**
   ```bash
   php -S 0.0.0.0:8080 -t public
   ```

4. Open:
   - `http://localhost:8080/` dashboard
   - `http://localhost:8080/settings?section=general`

## Apache2 Deployment Notes

- Point Apache `DocumentRoot` to `public/`.
- Ensure `mod_rewrite` is enabled and `AllowOverride All` is set for the site directory.
- Keep `.env`/secrets out of version control and supply config via environment variables.

## Architecture Guidelines

- Keep **all raw SQL and PDO interactions** in `src/db.php` wrappers.
- Keep **shared reusable logic** in `src/functions.php`.
- Add new settings modules by:
  1. adding key in `setting_sections()`
  2. creating the section form block in `public/settings/index.php`
  3. storing values through `save_settings()`
- Add new navigation items in `nav_items()` to keep menus centralized.

## Next Suggested Steps

- Add authentication and role-based access
- Add ESI OAuth flow + token storage tables
- Add sync job queue + import workers
- Split section UI blocks into dedicated templates/components as settings grow
