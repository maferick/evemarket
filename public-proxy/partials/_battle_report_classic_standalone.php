<?php if ($allianceSummary !== []): ?>
<section class="surface-primary mt-4">
    <?php include __DIR__ . '/_battle_report_classic.php'; ?>

    <?php if ($dataQualityNotes !== []): ?>
        <div class="mt-3 pt-2 border-t border-white/5">
            <?php foreach ($dataQualityNotes as $note): ?>
                <p class="text-[10px] text-slate-500 leading-relaxed"><?= proxy_e((string) $note) ?></p>
            <?php endforeach; ?>
        </div>
    <?php endif; ?>
</section>
<?php endif; ?>
