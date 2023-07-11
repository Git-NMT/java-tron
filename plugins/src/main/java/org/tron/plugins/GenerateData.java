package org.tron.plugins;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.util.encoders.Hex;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.StringUtil;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.plugins.utils.DBUtils;
import org.tron.plugins.utils.db.LevelDBIterator;
import org.tron.protos.Protocol;
import picocli.CommandLine;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Asset 折分
 * @author liukai
 * @since 2023/5/31.
 */
@Slf4j(topic = "gen")
@CommandLine.Command(name = "generate",
        description = "generate",
        exitCodeListHeading = "Exit Codes:%n",
        exitCodeList = {
                "0: successful",
                "1: failed"})
public class GenerateData implements Callable<Integer> {
    private static String DB_DIR = "/Users/liukai/workspaces/temp/output-directory/output-directory/";
    private static String GENERATE_DIR = "/Users/liukai/workspaces/tmp/generate_data/account_asset.txt";

  private static int count = 0;

  @CommandLine.Option(
          names = {"--src", "-s"},
          required = true,
          order = 3)
  private String dbDir;

  @CommandLine.Option(
          names = {"--dest", "-ds"},
          required = true,
          order = 4)
  private String generateDir;


  private static DB db;

  @Override
  public Integer call() throws Exception {
    DB_DIR = dbDir;
    GENERATE_DIR = generateDir;
    logger.info("dbDir: " + DB_DIR);
    logger.info("generateDir: " + GENERATE_DIR);
    execute();
    return 1;
  }

  /**
   * Account 库
   * @return
   */
  public static LevelDBIterator getLevelDBIterator(String dbName) throws IOException {
    return new LevelDBIterator(getDBIterator(dbName));
  }

  public static DBIterator getDBIterator(String dbName) throws IOException {
    Path path = Paths.get(DB_DIR, "database", dbName);
    if (db == null) {
      db = DBUtils.newLevelDb(path);
    }
    return db.iterator();
  }

  public static int execute() throws IOException {
    BufferedWriter accountFile = createWriteFileWriter();
    LevelDBIterator accountIterator = getLevelDBIterator("account");
    accountIterator.seekToFirst();
    logger.info("open account");
    while (count < 2_000_000) {
      count++;
      byte[] key = accountIterator.getKey();
      byte[] value = accountIterator.getValue();
      Protocol.Account account = Protocol.Account.parseFrom(value);
      Map<String, Long> allAssets = getAllAssets(account);
      if (allAssets.size() == 0) {
        continue;
      }
      writeAccountAndAssetId(key, allAssets, accountFile);
      accountIterator.next();
    }
    accountFile.flush();
    accountFile.close();
    return 1;
  }

  public static void writeAccountAndAssetId(byte[] key, Map<String, Long> allAssets, BufferedWriter accountFile) {
    for (Map.Entry<String, Long> entry : allAssets.entrySet()) {
      String assetKey = entry.getKey();
      if (assetKey == null) {
        break;
      }
      long longKey = Long.parseLong(assetKey);
      String data = Hex.toHexString(key) + ", " + "0x" + Long.toHexString(longKey);
      try {
        writeToFile(accountFile, data);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private static BufferedWriter createWriteFileWriter() throws IOException {
    File indexFile = new File(GENERATE_DIR);
    return new BufferedWriter(new FileWriter(indexFile));
  }

  private static void writeToFile(BufferedWriter accountFile, String data) throws IOException {
    accountFile.write(data);
    accountFile.newLine();
    // for test
//    accountFile.flush();

    if (count % 10000 == 0) {
      accountFile.flush();
    }
  }

  public static Map<String, Long> getAllAssets(Protocol.Account account) {
    Map<String, Long> assets = new HashMap<>();
    Map<WrappedByteArray, byte[]> map = prefixQuery(account.getAddress().toByteArray());
    map.forEach((k, v) -> {
      // 拆分的重点在这里
      byte[] assetID = ByteArray.subArray(k.getBytes(),
              account.getAddress().toByteArray().length, k.getBytes().length);
      String assetIdKey = ByteArray.toStr(assetID);
      if (StringUtils.isNoneEmpty(assetIdKey)) {
        assets.put(assetIdKey, Longs.fromByteArray(v));
      }
    });
    return assets;
  }

  public static Map<WrappedByteArray, byte[]> prefixQuery(byte[] key) {
    try (DBIterator iterator = getDBIterator("account-asset")) {
      Map<WrappedByteArray, byte[]> result = new HashMap<>();
      for (iterator.seek(key); iterator.hasNext(); iterator.next()) {
        Map.Entry<byte[], byte[]> entry = iterator.peekNext();
        if (Bytes.indexOf(entry.getKey(), key) == 0) {
          result.put(WrappedByteArray.of(entry.getKey()), entry.getValue());
        } else {
          return result;
        }
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

//  public static Map<WrappedByteArray, byte[]> prefixQuery(byte[] address) {
//    LevelDBIterator accountAssetIterator = null;
//    try {
//      accountAssetIterator = getLevelDBIterator("account-asset");
//      accountAssetIterator.seekToFirst();
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//    Map<WrappedByteArray, byte[]> result = new HashMap<>();
//    while (accountAssetIterator.hasNext()) {
//      byte[] key = accountAssetIterator.getKey();
//      byte[] value = accountAssetIterator.getValue();
//        if (Bytes.indexOf(key, address) == 0) {
//          logger.info("key: {}", WrappedByteArray.of(key));
//          result.put(WrappedByteArray.of(key), value);
//        } else {
//          return result;
//        }
//      accountAssetIterator.next();
//    }
//    return result;
//  }

  public static void main(String[] args) throws IOException {
    execute();
  }
}
