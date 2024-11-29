package org.atlantfs;

public class DirNotEnoughSpaceException extends NotEnoughSpaceException {

    public DirNotEnoughSpaceException() {
    }

    public DirNotEnoughSpaceException(String message) {
        super(message);
    }

    public DirNotEnoughSpaceException(String message, Throwable cause) {
        super(message, cause);
    }

    public DirNotEnoughSpaceException(Throwable cause) {
        super(cause);
    }

}
