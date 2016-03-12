package util

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, FileSystem}
import org.apache.hadoop.io.IOUtils

/**
 * Created by renienj on 2/27/16.
 */
object Helper {

  def copyMergeWithHeader(srcFS :FileSystem,srcDir: Path, dstFS: FileSystem, dstFile: Path,
                          deleteSource: Boolean, conf: Configuration, header: String): Boolean  = {

    checkDest(srcDir.getName(), dstFS, dstFile, false);
    if(!srcFS.getFileStatus(srcDir).isDir()) {
      return false;
    }
    else {
      val out = dstFS.create(dstFile);
      if (header != null) {
        out.write((header + "\n").getBytes("UTF-8"));
      }

      try {
        val contents = srcFS.listStatus(srcDir);

        for (i <- 0 until contents.length) {
          if (!contents(i).isDir()) {
            val in = srcFS.open(contents(i).getPath());

            try {
              IOUtils.copyBytes(in, out, conf, false);

            } finally {
              in.close();
            }
          }
        }
      } finally {
        out.close();
      }

      return if(deleteSource) srcFS.delete(srcDir, true) else true;
    }
  }

  @throws(classOf[IOException])
  def checkDest(srcName: String,dstFS: FileSystem,dst: Path,overwrite: Boolean): Path  = {
    if(dstFS.exists(dst)) {
      val sdst: FileStatus = dstFS.getFileStatus(dst);
      if(sdst.isDir()) {
        if(null == srcName) {
          throw new IOException("Target " + dst + " is a directory");
        }

        return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
      }

      if(!overwrite) {
        throw new IOException("Target " + dst + " already exists");
      }
    }
    return dst;
  }
}

