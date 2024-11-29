package org.atlantfs;

public class BitmapRegionNotEnoughSpaceException extends NotEnoughSpaceException {

    public BitmapRegionNotEnoughSpaceException() {
    }

    public BitmapRegionNotEnoughSpaceException(String message) {
        super(message);
    }

    public BitmapRegionNotEnoughSpaceException(String message, Throwable cause) {
        super(message, cause);
    }

    public BitmapRegionNotEnoughSpaceException(Throwable cause) {
        super(cause);
    }

}
