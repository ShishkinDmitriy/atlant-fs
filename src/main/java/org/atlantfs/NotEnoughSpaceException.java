package org.atlantfs;

public class NotEnoughSpaceException extends AtlantFileSystemException {
    public NotEnoughSpaceException() {
    }

    public NotEnoughSpaceException(String message) {
        super(message);
    }

    public NotEnoughSpaceException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotEnoughSpaceException(Throwable cause) {
        super(cause);
    }
}
