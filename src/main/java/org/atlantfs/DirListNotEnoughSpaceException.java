package org.atlantfs;

public class DirListNotEnoughSpaceException extends DirNotEnoughSpaceException {

    public DirListNotEnoughSpaceException() {
    }

    public DirListNotEnoughSpaceException(String message) {
        super(message);
    }

    public DirListNotEnoughSpaceException(String message, Throwable cause) {
        super(message, cause);
    }

    public DirListNotEnoughSpaceException(Throwable cause) {
        super(cause);
    }

}
