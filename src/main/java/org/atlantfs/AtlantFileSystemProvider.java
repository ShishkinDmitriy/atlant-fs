package org.atlantfs;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.ProviderMismatchException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AtlantFileSystemProvider extends FileSystemProvider {

    final Map<Path, AtlantFileSystem> fileSystems = new HashMap<>();

    @Override
    public String getScheme() {
        return "atlant";
    }

    @Override
    public AtlantFileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        synchronized (fileSystems) {
            String schemeSpecificPart = uri.getSchemeSpecificPart();
            int i = schemeSpecificPart.indexOf("!/");
            if (i >= 0) {
                schemeSpecificPart = schemeSpecificPart.substring(0, i);
            }
            AtlantFileSystem fileSystem = fileSystems.get(schemeSpecificPart);
            if (fileSystem != null) {
                throw new FileSystemAlreadyExistsException(schemeSpecificPart);
            }
            var atlant = Paths.get(schemeSpecificPart);
            fileSystem = new AtlantFileSystem(this, atlant, env);
            fileSystems.put(atlant, fileSystem);
            return fileSystem;
        }
    }

    @Override
    public AtlantFileSystem getFileSystem(URI uri) {
        synchronized (fileSystems) {
            String schemeSpecificPart = uri.getSchemeSpecificPart();
            int i = schemeSpecificPart.indexOf("!/");
            if (i >= 0) {
                schemeSpecificPart = schemeSpecificPart.substring(0, i);
            }
            var atlant = Paths.get(schemeSpecificPart);
            AtlantFileSystem fileSystem = fileSystems.get(atlant);
            if (fileSystem == null) {
                throw new FileSystemNotFoundException(schemeSpecificPart);
            }
            return fileSystem;
        }
    }

    @Override
    public Path getPath(URI uri) {
        String str = uri.getSchemeSpecificPart();
        int i = str.indexOf("!/");
        if (i == -1) {
            throw new IllegalArgumentException("URI: " + uri + " does not contain path info ex. github:apache/karaf#master!/");
        }
        return null;
//        return getFileSystem(uri, true).getPath(str.substring(i + 1));
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        if (!(path instanceof AtlantPath atlantPath)) {
            throw new ProviderMismatchException();
        }
        if (!atlantPath.isAbsolute()) {
            atlantPath = atlantPath.toAbsolutePath();
        }
        return atlantPath.getFileSystem().newByteChannel(atlantPath, options, attrs);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        if (!(dir instanceof AtlantPath atlantPath)) {
            throw new ProviderMismatchException();
        }
        if (!atlantPath.isAbsolute()) {
            atlantPath = atlantPath.toAbsolutePath();
        }
        return atlantPath.getFileSystem().newDirectoryStream(atlantPath, filter);
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        if (!(dir instanceof AtlantPath atlantPath)) {
            throw new ProviderMismatchException();
        }
        if (!atlantPath.isAbsolute()) {
            atlantPath = atlantPath.toAbsolutePath();
        }
        atlantPath.getFileSystem().createDirectory(atlantPath);
    }

    @Override
    public void delete(Path path) throws IOException {
        if (!(path instanceof AtlantPath atlantPath)) {
            throw new ProviderMismatchException();
        }
        if (!atlantPath.isAbsolute()) {
            atlantPath = atlantPath.toAbsolutePath();
        }
        atlantPath.getFileSystem().delete(atlantPath);
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {

    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {

    }

    @Override
    public boolean isSameFile(Path path, Path path2) {
        return path.toAbsolutePath().equals(path2.toAbsolutePath());
    }

    @Override
    public boolean isHidden(Path path) throws IOException {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        return null;
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        return null;
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        if (!(path instanceof AtlantPath atlantPath)) {
            throw new ProviderMismatchException();
        }
        //noinspection unchecked
        return (A) atlantPath.getFileSystem().readAttributes(atlantPath, options);
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        return Map.of();
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
    }

    void removeFileSystem(Path path) {
        synchronized (fileSystems) {
            fileSystems.remove(path);
        }
    }

}
