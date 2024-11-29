package org.atlantfs;

public class DataNotEnoughSpaceException extends AtlantFileSystemException {

    public DataNotEnoughSpaceException() {
    }

    public DataNotEnoughSpaceException(String message) {
        super(message);
    }

    public DataNotEnoughSpaceException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataNotEnoughSpaceException(Throwable cause) {
        super(cause);
    }

}
