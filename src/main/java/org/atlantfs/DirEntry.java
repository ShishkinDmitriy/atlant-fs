package org.atlantfs;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * Represent single directory entry.
 * <p>
 * Inspired by {@code ext4_dir_entry_2}.
 * <pre>
 *          name length  file type
 *                    |  |
 *  inode       len   v  v  name     padding
 * +-----------+-----+--+--+--------+--------------+
 * |40 E2 01 00|10 00|03|01|64 69 72|00 00 00 00 00|
 * +-----------+-----+--+--+--------+--------------+
 *  F1 FB 09 00 10 00 04 01 64 69 72 32 00 00 00 00
 * </pre>
 * <p>
 * Max length of entry is {@code 4 + 2 + 1 + 1 + 255 = 263}.
 *
 * @see <a href="https://blogs.oracle.com/linux/post/understanding-ext4-disk-layout-part-2">Understanding Ext4 Disk Layout, Part 2</a>
 * @see <a href="https://blogs.oracle.com/linux/post/space-management-with-large-directories-in-ext4">Space Management With Large Directories in Ext4</a>
 */
public class DirEntry {

    private static final Logger log = Logger.getLogger(DirEntry.class.getName());

    public static final int NAME_MAX_LENGTH = 255;

    /**
     * The length of {@code inode}, {@code length}, {@code nameLength}, {@code fileType} fields.
     * <p>
     * Valid entry (with non-empty name) should be strictly greater than this length.
     */
    public static final int ENTRY_MIN_LENGTH = 8;
    public static final int PADDING = 8;

    /**
     * Inode number.
     * <p>
     * 4 bytes.
     */
    private int inode;

    /**
     * Directory entry length.
     * <p>
     * 2 bytes.
     */
    private short length;

    /**
     * File type.
     * <p>
     * 1 byte.
     */
    private FileType fileType;

    /**
     * File name.
     * <p>
     * Up to 255 bytes.
     */
    private String name;

    /**
     * Position in block.
     * <p>
     * Not persisted.
     */
    private final transient int position;

    private transient boolean dirty;

    public DirEntry(int position) {
        this.position = position;
    }

    static DirEntry read(ByteBuffer buffer) {
        int initial = buffer.position();
        log.fine(() -> "Reading Dir entry [position=" + initial + "]...");
        DirEntry dirEntry = new DirEntry(initial);
        dirEntry.inode = buffer.getInt();
        dirEntry.length = buffer.getShort();
        if (dirEntry.length <= ENTRY_MIN_LENGTH) {
            throw new IllegalArgumentException("Too small Dir entry [length=" + dirEntry.length + "]");
        }
        if (dirEntry.inode == Inode.NULL) {
            dirEntry.fileType = FileType.UNKNOWN;
            dirEntry.name = ""; // To have 0 length
        } else {
            byte nameLength = buffer.get();
            dirEntry.fileType = FileType.read(buffer);
            byte[] chars = new byte[nameLength];
            buffer.get(chars);
            dirEntry.name = new String(chars);
        }
        log.finer(() -> "Successfully read Dir entry [entry=" + dirEntry + "]");
        buffer.position(dirEntry.position + dirEntry.length);
        return dirEntry;
    }

    void write(ByteBuffer buffer) {
        int initial = buffer.position();
        log.fine(() -> "Writing Dir entry [position=" + initial + "]...");
        buffer.putInt(inode);
        buffer.putShort(length);
        buffer.put((byte) name.length());
        fileType.write(buffer);
        buffer.put(name.getBytes());
        log.finer(() -> "Successfully written Dir entry [entry=" + this + "]");
    }

    /**
     * Called when the next Dir entry is deleted.
     *
     * @param other the Dir entry to merge
     */
    void merge(DirEntry other) {
        assert this.position + this.length == other.position : "Should be the next Dir entry";
        this.length += other.length;
    }

    void merge(DirEntry other, DirEntry next) {
        assert this.position + this.length == other.position : "Should be the next Dir entry";
        assert this.position + this.length + other.length == next.position : "Should be the next-next Dir entry";
        this.length += other.length;
        this.length += next.length;
    }

    void rename(String newName) {
        if (newName.length() > NAME_MAX_LENGTH) {
            throw new IllegalArgumentException("Name too long");
        }
    }

    int calculateMinPadding() {
        int size = ENTRY_MIN_LENGTH + name.length();
        return (PADDING - (size % PADDING)) % PADDING;
    }

    int calculateRealPadding() {
        return length - ENTRY_MIN_LENGTH - name.length();
    }

    boolean isEmpty() {
        return inode == Inode.NULL;
    }

    String getName() {
        return name;
    }

    int getInode() {
        return inode;
    }

    short getLength() {
        return length;
    }

    FileType getFileType() {
        return fileType;
    }

    int getPosition() {
        return position;
    }

    void setInode(int inode) {
        this.inode = inode;
    }

    void setLength(short length) {
        this.length = length;
    }

    void setFileType(FileType fileType) {
        this.fileType = fileType;
    }

    void setName(String name) {
        this.name = name;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public String toString() {
        return "DirEntry{" +
                "inode=" + inode +
                ", length=" + length +
                ", fileType=" + fileType +
                ", name='" + name + '\'' +
                ", position=" + position +
                '}';
    }

}
