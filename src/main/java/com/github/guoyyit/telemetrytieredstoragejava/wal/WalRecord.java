package com.github.guoyyit.telemetrytieredstoragejava.wal;

import com.github.guoyyit.telemetrytieredstoragejava.model.TelemetrySample;
import com.github.guoyyit.telemetrytieredstoragejava.storage.BlockMeta;

import java.util.List;

/**
 * One durable log entry. The WAL is the single source of truth for both
 * writes and tier migrations, which makes crash recovery a simple replay:
 * <ul>
 *   <li>PUT - a sample was committed to the hot tier (sample)</li>
 *   <li>ARCHIVE - ids moved from hot into a new cold block (block, ids)</li>
 *   <li>UNARCHIVE - a cold block was promoted back into the hot tier (block, samples).
 *       The samples are carried in the record itself so replay never depends on
 *       the block file still existing (a following DELETE_BLOCK may have
 *       removed it before the crash).</li>
 *   <li>DELETE_BLOCK - a cold block file was deleted (targetFile)</li>
 *   <li>REPLACE - a cold block was atomically replaced by another, e.g. downsampling (block=new, targetFile=old)</li>
 *   <li>DROP - samples were removed from hot without archiving, e.g. expired (ids)</li>
 * </ul>
 * Replay handlers are idempotent so a snapshot+WAL overlap is harmless.
 */
public record WalRecord(Type type, TelemetrySample sample, BlockMeta block,
                        List<String> ids, String targetFile, List<TelemetrySample> samples) {

    public enum Type { PUT, ARCHIVE, UNARCHIVE, DELETE_BLOCK, REPLACE, DROP }

    public static WalRecord put(TelemetrySample s) {
        return new WalRecord(Type.PUT, s, null, null, null, null);
    }

    public static WalRecord archive(BlockMeta block, List<String> ids) {
        return new WalRecord(Type.ARCHIVE, null, block, ids, null, null);
    }

    public static WalRecord unarchive(BlockMeta block, List<TelemetrySample> samples) {
        return new WalRecord(Type.UNARCHIVE, null, block, null, null, samples);
    }

    public static WalRecord deleteBlock(String targetFile) {
        return new WalRecord(Type.DELETE_BLOCK, null, null, null, targetFile, null);
    }

    public static WalRecord replace(BlockMeta newBlock, String oldFile) {
        return new WalRecord(Type.REPLACE, null, newBlock, null, oldFile, null);
    }

    public static WalRecord drop(List<String> ids) {
        return new WalRecord(Type.DROP, null, null, ids, null, null);
    }
}
