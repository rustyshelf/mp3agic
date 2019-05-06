package com.mpatric.mp3agic;

import com.upplication.s3fs.S3Path;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.OpenOption;

public class S3InputSource implements InputSource {

	private S3Path path;
	private S3ReadOnlySeekableByteChannel channel;

	S3InputSource(S3Path path) throws IOException{
		this.path = path;
		this.channel = new S3ReadOnlySeekableByteChannel(path);
	}

	@Override
	public String getResourceName() {
		return path.getKey();
	}

	@Override
	public boolean isReadable() {
		return true;
	}

	@Override
	public long getLength() throws IOException {
		return channel.size();
	}

	@Override
	public SeekableByteChannel openChannel(OpenOption... options) throws IOException {
		return channel.openStreamAt(0);
	}
}
