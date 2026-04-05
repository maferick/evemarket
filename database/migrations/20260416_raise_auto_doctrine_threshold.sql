-- Raise the default auto-doctrine activation threshold from 5 → 30.
--
-- Field experience with live killmail data shows that a threshold of 5
-- produces large amounts of noise: coincidental loadout overlaps
-- between 2-3 kills start registering as "doctrines". 30 losses in the
-- 30-day window is a much stronger signal that a fit is actually being
-- repeatedly fielded by the alliance.
--
-- Only update the default if the setting is still at the old shipped
-- value — servers that already hand-tuned to something else keep their
-- local preference.
UPDATE app_settings
   SET setting_value = '30'
 WHERE setting_key = 'auto_doctrines.min_losses_threshold'
   AND setting_value = '5';
