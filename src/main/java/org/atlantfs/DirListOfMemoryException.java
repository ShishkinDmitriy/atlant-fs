package org.atlantfs;

public class DirListOfMemoryException extends DirOutOfMemoryException {

    public DirListOfMemoryException() {
    }

    public DirListOfMemoryException(String message) {
        super(message);
    }

    public DirListOfMemoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public DirListOfMemoryException(Throwable cause) {
        super(cause);
    }

}
