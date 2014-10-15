package org.bitsofinfo.s3.master;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.bitsofinfo.s3.toc.TocInfo;

/**
 * Proxies 2 underlying queues, one for large files
 * and the other for everything else, this is to 
 * ensure the distribution of when large files are sent
 * out to the main TOCQueue @ SQS is more broken up and gives
 * preference that large files go out earlier than
 * all other smaller things.
 *
 */
public class TocInfoSizeAwareQueue implements Queue<TocInfo> {
	
	private long largeFileMinSizeBytes = 0;
	private Queue<TocInfo> tocQueue = new ConcurrentLinkedQueue<TocInfo>();
	private Queue<TocInfo> largeFileTocQueue = new ConcurrentLinkedQueue<TocInfo>();
	private long largeFileLastPolled = System.currentTimeMillis();
	private long ensureLargeFilePolledMinMS = 1000;
	
	public TocInfoSizeAwareQueue(long largeFileMinSizeBytes) {
		this.largeFileMinSizeBytes = largeFileMinSizeBytes;
	}


	@Override
	public TocInfo poll() {
	
		long now = System.currentTimeMillis();
		
		// if tocQueue empty go right to large file one
		if (tocQueue.isEmpty()) {
			largeFileLastPolled = now;
			return largeFileTocQueue.poll();
			
		// if largeFileTocQueue is NOT empty, and its been longer than our min large file send time
		// poll from the large files
		} else if (!largeFileTocQueue.isEmpty() && (now - largeFileLastPolled) > ensureLargeFilePolledMinMS) {
			largeFileLastPolled = now;
			return largeFileTocQueue.poll();
		}
		
		// otherwise just poll from tocQueue
		return tocQueue.poll();
	}

	
	@Override
	public boolean addAll(Collection<? extends TocInfo> c) {
		for (TocInfo ti : c) {
			if (!ti.isDirectory() && ti.getSize() > this.largeFileMinSizeBytes) {
				largeFileTocQueue.add(ti);
			} else {
				tocQueue.add(ti);
			}
		}
		return true;
	}
	

	@Override
	public int size() {
		return (tocQueue.size() + largeFileTocQueue.size());
	}


	@Override
	public boolean add(TocInfo ti) {
		if (!ti.isDirectory() && ti.getSize() > this.largeFileMinSizeBytes) {
			return largeFileTocQueue.add(ti);
		} else {
			return tocQueue.add(ti);
		}
	}

	@Override
	public boolean isEmpty() {
		return (tocQueue.isEmpty() && largeFileTocQueue.isEmpty());
	}
	
	@Override
	public void clear() {
		tocQueue.clear();
		largeFileTocQueue.clear();
	}

	@Override
	public boolean contains(Object o) {
		return tocQueue.contains(o) || largeFileTocQueue.contains(o);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}


	@Override
	public Iterator<TocInfo> iterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] toArray() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException();
	}


	@Override
	public TocInfo element() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean offer(TocInfo e) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TocInfo peek() {
		throw new UnsupportedOperationException();
	}

	@Override
	public TocInfo remove() {
		throw new UnsupportedOperationException();
	}


}
