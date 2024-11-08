package org.atlantfs;

public class BitmapRegionOutOfMemoryException extends AtlantFileSystemException {

    public BitmapRegionOutOfMemoryException() {
    }

    public BitmapRegionOutOfMemoryException(String message) {
        super(message);
    }

    public BitmapRegionOutOfMemoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public BitmapRegionOutOfMemoryException(Throwable cause) {
        super(cause);
    }

}
