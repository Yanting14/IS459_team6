import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Test-only local filesystem for Windows. Hadoop's own needs winutils.exe and hadoop.dll
 * (for chmod and directory listing); this one skips permissions and lists with java.io.
 * conftest.py compiles it and sets it as fs.file.impl on Windows only.
 */
public class WindowsLocalFileSystem extends LocalFileSystem {
    public WindowsLocalFileSystem() {
        super(new Raw());
    }

    static class Raw extends RawLocalFileSystem {
        @Override
        public void setPermission(Path p, FsPermission permission) {
        }

        @Override
        public FileStatus[] listStatus(Path f) throws IOException {
            File local = pathToFile(f);
            if (!local.exists()) {
                throw new FileNotFoundException("File " + f + " does not exist");
            }
            if (local.isFile()) {
                return new FileStatus[] {getFileStatus(f)};
            }
            List<FileStatus> out = new ArrayList<>();
            for (String name : local.list()) {
                try {
                    out.add(getFileStatus(new Path(f, name)));
                } catch (FileNotFoundException e) {
                    // deleted while listing
                }
            }
            return out.toArray(new FileStatus[0]);
        }
    }
}
