package com.mpatric.mp3agic;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Throwables;
import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3Path;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;


public class S3ReadOnlySeekableByteChannel implements SeekableByteChannel {

	private static final int DEFAULT_BUFFER_SIZE = 64000;

	private S3Path path;
	private long length;
	private ExtBufferedInputStream bufferedStream;
	private ReadableByteChannel rbc;
	private long position = 0;

	/**
	 * Open or creates a file, returning a seekable byte channel
	 *
	 * @param path    the path open or create
	 * @throws IOException if an I/O error occurs
	 */
	public S3ReadOnlySeekableByteChannel(S3Path path) throws IOException {
		this.path = path;

		String key = path.getKey();
		System.out.println("Key: " + key);
		try {
			S3FileSystem fileSystem = path.getFileSystem();
			System.out.println("fileSystem: " + fileSystem);
			String bucketName = path.getFileStore().getBucket().getName();
			System.out.println("bucketName: " + bucketName);
			
			this.length = fileSystem.getClient().getObjectMetadata(bucketName, key).getContentLength();
		}
		catch (AmazonS3Exception e) {
			throw new FileNotFoundException("Unable to locate file");
		}


	}


	S3ReadOnlySeekableByteChannel openStreamAt(long position) throws IOException {
		if (rbc != null) { rbc.close(); }

		GetObjectRequest rangeObjectRequest = new GetObjectRequest(path.getFileStore().getBucket().getName(), path.getKey()).withRange(position);
		S3Object s3Object = path.getFileSystem().getClient().getObject(rangeObjectRequest);
		bufferedStream = new ExtBufferedInputStream(s3Object.getObjectContent(), DEFAULT_BUFFER_SIZE);
		rbc = Channels.newChannel(bufferedStream);
		this.position = position;

		return this;
	}

	public boolean isOpen() {
		return rbc.isOpen();
	}

	public long position() throws IOException {
		return position;
	}

	public SeekableByteChannel position(long targetPosition) throws IOException {
		long offset = targetPosition - position();
		if (offset > 0 && offset < bufferedStream.getBytesInBufferAvailable()) {
			long skipped = bufferedStream.skip(offset);
			if (skipped != offset) {
				// shouldn't happen since we are within the buffer
				throw new IOException("Could not seek to " + targetPosition);
			}

			position += offset;
		}
		else if (offset != 0) {
			openStreamAt(targetPosition);
		}

		return this;
	}

	public int read(ByteBuffer dst) throws IOException {
		int n = rbc.read(dst);
		if (n > 0) {
			position += n;
		}

		return n;
	}

	public SeekableByteChannel truncate(long size) throws IOException {
		throw new NonWritableChannelException();
	}

	public int write (ByteBuffer src) throws IOException {
		throw new NonWritableChannelException();
	}

	public long size() throws IOException {
		return length;
	}

	public void close() throws IOException {
		rbc.close();
	}

	private class ExtBufferedInputStream extends BufferedInputStream {
		private ExtBufferedInputStream(final InputStream inputStream, final int i) {
			super(inputStream, i);
		}

		/** Returns the number of bytes that can be read from the buffer without reading more into the buffer. */
		int getBytesInBufferAvailable() {
			if (this.count == this.pos) return 0;
			else return this.buf.length - this.pos;
		}
	}

}
