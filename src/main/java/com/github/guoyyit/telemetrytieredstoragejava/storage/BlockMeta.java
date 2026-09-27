package com.github.guoyyit.telemetrytieredstoragejava.storage;

/**
 * Metadata of one cold archive block. Blocks belong to exactly one stream and
 * cover a contiguous-ish time range; they are the unit of compression,
 * migration and retention deletion.
 */
public record BlockMeta(String fileName, String streamId, long minTs, long maxTs,
                        int count, boolean downsampled) {
}
