/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package tassl.automate.overlay.chord;

import java.util.Map.Entry;
import programming5.collections.MultiVector;

/**
 * Utility class for chord that optimizes the resolution of indices by caching previous resolution calls.
 * Provides methods for invalidating cache entries affected by changes in the overlay structure.
 * @author aquirozh
 */
public class ChordResolveCache {

    protected MultiVector<ChordID, ChordNodeInfo> cache = new MultiVector<ChordID, ChordNodeInfo>();

    /**
     * Inserts a query/solution pair into the cache
     * @param query the chord id that was resolved
     * @param resolution the info of the node found to hold the given chord id
     */
    public synchronized void insertResolvePair(ChordID query, ChordNodeInfo resolution) {
        int currentIndex = cache.indexOfInSecond(resolution);
        if (currentIndex >= 0) {    // Entry found
            ChordID currentQuery = cache.getInFirstAt(currentIndex);
            if (currentQuery.isBetween(query, resolution.getChordID())) {
                cache.setInFirstAt(currentIndex, query);
            }
        }
        else {
            cache.add(query, resolution);
        }
    }

    /**
     * A query can be resolved from the cache if a valid entry is found where the unknown query index 
     * is between the known query index and the resolution index
     * @param query the chord id to resolve
     * @return the info of the node that resolves the query if found in the cache, or null otherwise
     */
    public synchronized ChordNodeInfo findResolution(ChordID query) {
        ChordNodeInfo ret = null;
        for (Entry<ChordID, ChordNodeInfo> entry : cache.entrySet()) {
            if (query.isBetweenIntervalRight(entry.getKey(), entry.getValue().getChordID()) || query.equals(entry.getKey())) {
                ret = entry.getValue();
                break;
            }
        }
        return ret;
    }

    /**
     * Invalidate any entries if a new node is contained between the query index and the resolution 
     * index if the new node cannot be determined to be a valid successor to the entry query
     * @param unresolvedNode the chord id of the new node
     * @return true if any entries were invalidated; false otherwise
     */
    public synchronized boolean invalidateAfterAdd(ChordID unresolvedNode) {
        boolean invalidated = false;
        for (int i = 0; i < cache.size(); i++) {
            ChordID currentQuery = cache.getInFirstAt(i);
            ChordNodeInfo currentResolution = cache.getInSecondAt(i);
            if (unresolvedNode.isBetweenIntervalRight(currentQuery, currentResolution.getChordID()) || unresolvedNode.equals(currentQuery)) {
                cache.remove(i);
                invalidated = true;
                break;
            }
        }
        return invalidated;
    }

    /**
     * Update any entries if a new node is contained between the query index and the resolution
     * index if the new node is determined to be a valid successor to the entry query
     * @param newNode the chord info of the new node
     * @return true if any entries were updated; false otherwise
     */
    public synchronized boolean updateAfterAdd(ChordNodeInfo newNode) {
        boolean updated = false;
        for (int i = 0; i < cache.size(); i++) {
            ChordID currentQuery = cache.getInFirstAt(i);
            ChordNodeInfo currentResolution = cache.getInSecondAt(i);
            if (newNode.getChordID().isBetweenIntervalRight(currentQuery, currentResolution.getChordID()) || newNode.getChordID().equals(currentQuery)) {
                cache.setInSecondAt(i, newNode);
                updated = true;
                break;
            }
        }
        return updated;
    }

    /**
     * Invalidate any entries if a node contained in a cache entry corresponds to a node known to have
     * failed or left
     * @param removedNode the chord id of the removed node
     * @return true if any entries were invalidated; false otherwise
     */
    public synchronized boolean invalidateAfterRemove(ChordID removedNode) {
        boolean invalidated = false;
        for (int i = 0; i < cache.size(); i++) {
            ChordID cachedResolution = cache.getInSecondAt(i).getChordID();
            if (cachedResolution.equals(removedNode)) {
                cache.remove(i);
                invalidated = true;
                break;
            }
        }
        return invalidated;
    }

}
