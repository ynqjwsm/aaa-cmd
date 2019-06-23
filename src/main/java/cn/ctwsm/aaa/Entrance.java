package cn.ctwsm.aaa;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class Entrance {

    /**
     * Apache Commons CLI 命令解析
     * @param args
     * @return
     * @throws ParseException
     */
    private static CommandLine parseArg(String[] args) throws ParseException {
        //define args
        Options options = new Options();
        options.addOption("h", "help",false, "display help menu.");
        options.addOption("t", "ttl",true, "define the ttl(time to live in seconds) for record.");
        options.addOption("s", "source",true, "define the source folder.");
        options.addOption("d", "delete",false, "delete source file after parse.");
        options.addOption("i", "ip",true, "redis ip.");
        options.addOption("p", "port",true, "redis pord.");
        //options.addOption("d", "database",true, "define the redis database name.");
        options.addOption("a", "auth",true, "pass word for redis.");
        options.addOption("f", "finish-suffix",true, "finish file suffix.");

        //parse args
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        //display tips
        if (commandLine.hasOption("help") || commandLine.hasOption("h")) {
            HelpFormatter hf = new HelpFormatter();
            hf.setWidth(110);
            hf.printHelp("aaa-cmd", options, true);
            System.exit(-1);
        }

        //check args.
        String[][] required = new String[][]{
                {"s", "source folder must be provided!"},
                {"i", "ip of database must be provided!"},
                {"p", "port of database must be provided!"}
                //{"d", "database name must be provided!"}
        };
        for(String[] pair : required){
            if (!commandLine.hasOption(pair[0])) {
                throw new IllegalArgumentException(pair[1]);
            }
        }

        return commandLine;
    }

    public static void main(String[] args) throws Exception {
        // args
        CommandLine commandLine = null;
        try {
            commandLine = parseArg(args);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //load args
        Boolean needDelete = commandLine.hasOption("d") ? Boolean.TRUE : Boolean.FALSE;
        Boolean needAuth = commandLine.hasOption("a");
        Integer ttl = commandLine.hasOption("t") ? Integer.parseInt(commandLine.getOptionValue("t")) : -1;
        String sourceFolder = commandLine.getOptionValue("s");
        String ipOfRedis = commandLine.getOptionValue("i");
        Integer portOfRedis = Integer.parseInt(commandLine.getOptionValue("p"));
        //String databaseOfRedis = commandLine.getOptionValue("d");
        String passOfRedis = commandLine.hasOption("a") ? commandLine.getOptionValue("a") : null;
        final String finishFileSuffix = commandLine.hasOption("f") ? commandLine.getOptionValue("f") : "OK";

        //redis conn
        Jedis jedis = new Jedis(ipOfRedis, portOfRedis);
        if(needAuth){
            jedis.auth(passOfRedis);
        }
        jedis.connect();

        /**
         * 将目录中符合条件的文件列出，需同时满足
         * 1. 后缀名为 txt 的文件
         * 2. 文件 size 大于0的文件
         * 3. 文件的 ok 标志文件存在
         */
        Collection<File> files = FileUtils.listFiles(new File(sourceFolder),
                new AndFileFilter(
                        new SuffixFileFilter("txt"),
                        new AndFileFilter(
                                new SizeFileFilter(1L),
                                new IOFileFilter() {
            public boolean accept(File file) {
                return new File(FilenameUtils.removeExtension(file.getAbsolutePath()) + finishFileSuffix).exists();
            }
            public boolean accept(File dir, String name) {
                return accept(FileUtils.getFile(dir, name));
            }
        })), TrueFileFilter.INSTANCE);

        /**
         * 将符合条件的文件按生成日期时间升序排列
         */
        List<File> fileList = new ArrayList<File>(files);
        Collections.sort(fileList, new Comparator<File>() {
            public int compare(File o1, File o2) {
                String baseName1 = FilenameUtils.getBaseName(o1.getName());
                String baseName2 = FilenameUtils.getBaseName(o2.getName());
                return Integer.parseInt(baseName1.substring(baseName1.lastIndexOf('+') + 1 + 4)) -
                        Integer.parseInt(baseName2.substring(baseName2.lastIndexOf('+') + 1 + 4));
            }
        });

        for (File file: fileList){
            parseAndInsert(jedis, FileUtils.readFileToByteArray(file), ttl);
            if(needDelete){
                file.delete();
                new File(FilenameUtils.removeExtension(file.getAbsolutePath()) + finishFileSuffix).delete();
            }
        }
        //close redis
        jedis.close();
    }

    /**
     * 用于将文件内容解析并写入redis
     * @param db jedis 实例
     * @param input 输入文件
     * @param ttl 条目生存周期（秒）
     * @throws IOException
     */
    public static void parseAndInsert(Jedis db, byte[] input, Integer ttl) throws IOException {
        LineIterator iterator = IOUtils.lineIterator(new ByteArrayInputStream(input), "UTF-8");
        outer:
        while (iterator.hasNext()){
            String line = iterator.next();
            char[] chars = line.toCharArray();
            int index = 0, accEnd = 0;
            // Account Part
            while (chars[index] != '|'){
                index++;
            }
            // if no account continue
            accEnd = index;
            if(accEnd == 0) continue;
            //skip a separator
            index ++;
            // Ip Part
            int ipEnd = 0,pointCnt = 0, digPart = 0;
            boolean afterPoint = true;
            while (chars[index] != '|'){
                if (CharUtils.isAsciiNumeric(chars[index]) && afterPoint && chars[index] > '0'){
                    digPart ++;
                    afterPoint = false;
                }else if(chars[index] == '.'){
                    pointCnt++;
                    afterPoint = true;
                }
                index++;
            }
            ipEnd = index;
            if(accEnd == ipEnd || pointCnt != 3 || digPart !=4) continue;
            String account = new String(chars, 0, accEnd).trim();
            String ip = new String(chars, accEnd + 1, ipEnd - accEnd - 1).trim();
            if (ttl > 0){
                db.setex(ip, ttl, account);
            }else {
                db.set(ip, account);
            }
        }
        iterator.close();
    }

}
