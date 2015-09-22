package com.dj;



import com.google.common.collect.Lists;
import com.google.common.io.LimitInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Created by jiangyu on 9/21/15.
 */
public class ImageSummary {
    public static final Logger LOG = LoggerFactory.getLogger(ImageSummary.class);
    public static int FILE_NUMBER = 0;
    public static int DIR_NUMBER = 0;

    public static void PARSE(RandomAccessFile file) throws IOException {
        if (!FSImageUtil.checkFileFormat(file)) {
            throw new IOException("Unrecognized FSImage");
        }
        FsImageProto.FileSummary summary = FSImageUtil.loadSummary(file);
        FileInputStream fin = null;

        try {
            fin = new FileInputStream(file.getFD());

            ArrayList<FsImageProto.FileSummary.Section> sections = Lists.newArrayList(summary
                    .getSectionsList());
            Collections.sort(sections, new Comparator<FsImageProto.FileSummary.Section>() {
                @Override
                public int compare(FsImageProto.FileSummary.Section s1, FsImageProto.FileSummary.Section s2) {
                    FSImageFormatProtobuf.SectionName n1 = FSImageFormatProtobuf.SectionName.fromString(s1.getName());
                    FSImageFormatProtobuf.SectionName n2 = FSImageFormatProtobuf.SectionName.fromString(s2.getName());
                    if (n1 == null) {
                        return n2 == null ? 0 : -1;
                    } else if (n2 == null) {
                        return -1;
                    } else {
                        return n1.ordinal() - n2.ordinal();
                    }
                }
            });

            for (FsImageProto.FileSummary.Section s : sections) {
                fin.getChannel().position(s.getOffset());
                InputStream is = FSImageUtil.wrapInputStreamForCompression(new Configuration(),
                        summary.getCodec(), new BufferedInputStream(new LimitInputStream(
                                fin, s.getLength())));

                switch (FSImageFormatProtobuf.SectionName.fromString(s.getName())) {
                    case NS_INFO:
                        loadNameSystemSection(is);
                        break;
                    case INODE:
                        loadINodeSection(is);
                        break;
                    default:
                        break;
                }
            }
        } finally {
            IOUtils.cleanup(null, fin);
        }
    }

    public static void loadNameSystemSection(InputStream in) throws IOException {
        FsImageProto.NameSystemSection ns = FsImageProto.NameSystemSection.parseDelimitedFrom(in);
        LOG.info("Loading NS Section");
        LOG.info("timestampV1:" + String.valueOf(ns.getGenstampV1()));
        LOG.info("timestampV2:" + String.valueOf(ns.getGenstampV2()));
        LOG.info("LastBlockId:" + String.valueOf(ns.getLastAllocatedBlockId()));
        LOG.info("TransactionId:" + String.valueOf(ns.getTransactionId()));
    }

    private static void loadINodeSection(InputStream in) throws IOException {
        FsImageProto.INodeSection s = FsImageProto.INodeSection.parseDelimitedFrom(in);
        LOG.info("Loading " + s.getNumInodes() + " INodes.");
        for (int i = 0; i < s.getNumInodes(); ++i) {
            FsImageProto.INodeSection.INode p = FsImageProto.INodeSection.INode.parseDelimitedFrom(in);
            if (p.getId() == INodeId.ROOT_INODE_ID) {
                ImageSummary.DIR_NUMBER++;
            } else {
                loadINode(p);
            }
        }
        LOG.info("File Number:" + ImageSummary.FILE_NUMBER);
        LOG.info("Dir Number:" + ImageSummary.DIR_NUMBER);
    }

    private static void loadINode(FsImageProto.INodeSection.INode n) {
        switch (n.getType()) {
            case FILE:
                ImageSummary.FILE_NUMBER++;
                break;
            case DIRECTORY:
                ImageSummary.DIR_NUMBER++;
                break;
            case SYMLINK:
                break;
            default:
                break;
        }
    }

    public static void main(String[] args) throws IOException {
        RandomAccessFile file = new RandomAccessFile(args[0], "r");
        ImageSummary.PARSE(file);
    }
}
