package org.atlantfs;

import java.io.IOException;

public class AtlantFileSystemException extends IOException {

    public AtlantFileSystemException() {
    }

    public AtlantFileSystemException(String message) {
        super(message);
    }

    public AtlantFileSystemException(String message, Throwable cause) {
        super(message, cause);
    }

    public AtlantFileSystemException(Throwable cause) {
        super(cause);
    }

}
