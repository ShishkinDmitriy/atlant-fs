package org.atlantfs;

public class DataBitmapNotEnoughSpaceException extends AtlantFileSystemException {

    public DataBitmapNotEnoughSpaceException() {
    }

    public DataBitmapNotEnoughSpaceException(String message) {
        super(message);
    }

    public DataBitmapNotEnoughSpaceException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataBitmapNotEnoughSpaceException(Throwable cause) {
        super(cause);
    }

}
