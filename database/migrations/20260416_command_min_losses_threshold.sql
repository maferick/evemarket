-- Seed the new ``auto_doctrines.command_min_losses_threshold`` setting.
--
-- Command Ships (group 540) and Command Destroyers (group 1534) almost
-- always fly in pairs (one shield/info, one armor/skirmish) and die
-- rarely. The 30-loss subcap floor hides them entirely and even the
-- 5-loss capital floor is too strict — a single lost pair is enough
-- signal that a booster doctrine is being actively fielded. Default
-- to 2 so one bad fight flips the pair into an active doctrine.
INSERT INTO app_settings (setting_key, setting_value) VALUES
    ('auto_doctrines.command_min_losses_threshold', '2')
ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value);
